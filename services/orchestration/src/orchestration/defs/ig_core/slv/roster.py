"""`silver_ig_roster` — the roster published for the pipeline to read.

The roster arrives as bronze snapshots (`ig_roster_raw`, append-only, one file
per `fetched_at`). This asset publishes the LATEST snapshot into DuckDB so the
pipeline's roster consumers (`labels`, `schedules`, `profiles`, `dims`) read the
roster from the lake instead of opening the dashboard's `ops.sqlite`.

The dependency direction this establishes is the point of the cutover: the
dashboard owns the roster and serves it; the pipeline lands and publishes a copy.
The pipeline never reads the dashboard's database, and the dashboard never
imports the pipeline.

Degrades to stale, never to broken: with no new snapshot (dashboard down, or the
roster unchanged) the previously published rows stay in place. A full DELETE +
INSERT inside one transaction keeps the table a faithful mirror of exactly one
snapshot — never a merge of several.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl
from dagster import AssetKey, asset

from orchestration.defs.ig_core.bnz.roster import latest_roster_path
from orchestration.defs.ig_core.slv.posts import ensure_state_tables
from orchestration.defs.platform.resources import DuckDBResource

logger = logging.getLogger(__name__)

_TABLE = "silver_ig_roster"


@asset(
    name="silver_ig_roster",
    group_name="instagram",
    deps=[AssetKey(["ig_roster_raw"])],
    description=(
        "The latest roster snapshot published to DuckDB. The pipeline's one "
        "source for creators/profiles; the dashboard owns the original."
    ),
)
def ig_roster_slv(duckdb: DuckDBResource) -> pl.DataFrame:
    """Publish the latest roster snapshot into state DuckDB."""
    ensure_state_tables(duckdb)

    snapshot = latest_roster_path()
    if snapshot is None:
        # No snapshot has ever landed AND the upstream asset failed. The
        # asset above is the one that raises; here the honest move is to
        # LEAVE the table as it is rather than truncate it into silence.
        logger.warning(
            "no roster snapshot found — leaving %s unchanged (stale, not empty)",
            _TABLE,
        )
        return pl.DataFrame()

    df = pl.read_parquet(snapshot)
    fetched_at = None
    meta_path = snapshot.with_suffix(".parquet.meta")
    if meta_path.exists():
        import json

        fetched_at = json.loads(meta_path.read_text(encoding="utf-8")).get("fetched_at")
    fetched_at = fetched_at or snapshot.stem

    df = df.with_columns(
        pl.lit(fetched_at).alias("source_fetched_at"),
        pl.lit(datetime.now(UTC).replace(tzinfo=None)).alias("processed_on"),
    )

    with duckdb.get_connection() as conn:
        # One transaction: the table is a mirror of ONE snapshot, never a
        # union across snapshots (a union would resurrect disabled profiles).
        cols = [c for c in df.columns]
        collist = ", ".join(f'"{c}"' for c in cols)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(f"DELETE FROM {_TABLE}")
            conn.register("_roster_df", df.to_arrow())
            # Explicit column list: the Parquet frame's order is the landing
            # order and need not match the table's DDL order.
            conn.execute(
                f"INSERT INTO {_TABLE} ({collist}) SELECT {collist} FROM _roster_df"
            )
            conn.unregister("_roster_df")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    logger.info(
        "published %s from %s (%d profiles, fetched_at=%s)",
        _TABLE,
        snapshot.name,
        df.height,
        fetched_at,
    )
    return df


__all__ = ["ig_roster_slv"]


# ── Readers (the pipeline's roster access) ─────────────────────────────────
#
# These replace direct `opsdb.roster` reads in the pipeline. The roster the
# pipeline acts on is the PUBLISHED one, so an operator changing the roster in
# the dashboard cannot change what the next scheduled scrape does mid-flight —
# it takes effect at the next `ig_roster_slv` publish.


def enabled_profiles(conn, *, platform: str = "instagram") -> list[dict]:
    """Enabled roster profiles from `silver_ig_roster`, ordered by handle.

    Mirrors `opsdb.roster.enabled_profiles`' contract (ad-hoc profiles excluded)
    but reads the published table. Returns `[]` when nothing has been published
    yet — the caller decides whether that is a skip or an error.
    """
    from opsdb.roster import AD_HOC_LIMIT

    cursor = conn.execute(
        f"SELECT platform, handle, profile_url, results_type, results_limit, tier "
        f"FROM {_TABLE} WHERE enabled = TRUE AND platform = ? AND results_limit != ? "
        "ORDER BY handle",
        [platform, AD_HOC_LIMIT],
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def creator_map(conn, *, platform: str = "instagram") -> dict[str, dict]:
    """`handle` → `{creator_id, creator_name}` from the published roster.

    Mirrors `opsdb.roster.creator_map`: the handle is normalised the same way
    (lowercased, leading `@` stripped) so lookups key identically to the
    source-of-truth implementation.
    """
    cursor = conn.execute(
        f"SELECT handle, creator_id, creator_name FROM {_TABLE} WHERE platform = ?",
        [platform],
    )
    out: dict[str, dict] = {}
    for handle, cid, cname in cursor.fetchall():
        if not handle:
            continue
        out[handle.lower().lstrip("@")] = {"creator_id": cid, "creator_name": cname}
    return out

