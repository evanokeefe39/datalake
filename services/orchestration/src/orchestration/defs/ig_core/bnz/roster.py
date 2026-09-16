"""Instagram roster bronze — the dashboard's creator roster landed as a source.

The dashboard OWNS `creators`/`profiles`. The pipeline needs that list to decide
WHAT to scrape and to label posts with creator identity, and it gets it the only
way that respects the ownership boundary: over the dashboard's HTTP API. It
never opens `data/ops.sqlite` directly — that is the coupling the roster cutover
removes, and re-introducing it here (a "just read the sqlite file" shortcut)
would put two services on one database again.

Shape: APPEND-ONLY snapshots under `<BRONZE>/ig_roster_raw/<fetched_at>.parquet`,
with the API response's `fetched_at` as the partition/provenance identity. An
unchanged `fetched_at` writes nothing, so a re-run of the same roster is a
no-op rather than a duplicate snapshot.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
from dagster import asset

from orchestration.defs.platform.paths import BRONZE_LAKE

logger = logging.getLogger(__name__)

#: Where the dashboard API lives. Compose points this at the service name; a
#: native run uses the dashboard's default loopback port.
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://127.0.0.1:3002")

#: The roster snapshot root, distinct from the flat bronze Parquet convention:
#: a roster is a SERIES of snapshots (history matters — a creator that was
#: enabled last month and disabled today is a real fact), where posts bronze is
#: one file per dataset.
ROSTER_BRONZE_DIR = BRONZE_LAKE / "ig_roster_raw"

_PROFILE_COLUMNS: dict[str, pl.DataType] = {
    "platform": pl.String,
    "handle": pl.String,
    "profile_url": pl.String,
    "results_type": pl.String,
    "results_limit": pl.Int64,
    "enabled": pl.Boolean,
    "tier": pl.String,
    "creator_id": pl.Int64,
    "creator_name": pl.String,
    "updated_at": pl.String,
}


def _snapshot_path(fetched_at: str) -> Path:
    """One Parquet per snapshot, named by the API's own timestamp.

    `fetched_at` is ISO-8601 with colons, which are illegal in Windows
    filenames — sanitised deterministically so the same fetch maps to the same
    file on both platforms.
    """
    safe = fetched_at.replace(":", "-").replace("+", "_")
    return ROSTER_BRONZE_DIR / f"{safe}.parquet"


@asset(
    name="ig_roster_raw",
    group_name="instagram",
    description=(
        "Append-only snapshots of the dashboard-owned creator roster, fetched "
        "over the dashboard API (never by opening ops.sqlite)."
    ),
)
def ig_roster_raw() -> pl.DataFrame:
    """Fetch the roster from the dashboard and land one immutable snapshot.

    Loud on failure: a non-200 or a transport error RAISES. The pipeline's
    fallback is the last landed snapshot (stale), never a silent empty frame —
    an empty roster would read downstream as "no creators configured" and
    quietly stop all scraping.
    """
    url = f"{DASHBOARD_URL}/api/roster"
    try:
        resp = httpx.get(url, timeout=30.0)
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"roster fetch failed: dashboard at {DASHBOARD_URL} is unreachable "
            f"({exc}); the pipeline uses the last landed snapshot rather than "
            "inventing an empty roster"
        ) from exc
    if resp.status_code != 200:
        raise RuntimeError(
            f"roster fetch failed: {url} returned HTTP {resp.status_code} — "
            f"{resp.text[:200]}"
        )

    payload = resp.json()
    for key in ("fetched_at", "profiles"):
        if key not in payload:
            raise RuntimeError(f"roster response missing {key!r}: {list(payload)}")

    fetched_at = payload["fetched_at"]
    creators = {c["id"]: c.get("name") for c in payload.get("creators", [])}

    rows = [
        {**{k: p.get(k) for k in _PROFILE_COLUMNS if k != "creator_name"},
         "creator_name": creators.get(p.get("creator_id"))}
        for p in payload["profiles"]
    ]
    df = pl.DataFrame(rows, schema=_PROFILE_COLUMNS) if rows else pl.DataFrame(
        schema=_PROFILE_COLUMNS
    )

    ROSTER_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    dest = _snapshot_path(fetched_at)
    if dest.exists():
        # Same fetched_at ⇒ the same roster ⇒ nothing new to land.
        logger.info("roster snapshot already landed: %s", dest.name)
        return pl.read_parquet(dest)

    df.write_parquet(dest)
    dest.with_suffix(".parquet.meta").write_text(
        json.dumps(
            {
                "source": url,
                "fetched_at": fetched_at,
                "landed_at": datetime.now(UTC).isoformat(),
                "profile_count": df.height,
                "creator_count": len(creators),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info("landed roster snapshot %s (%d profiles)", dest.name, df.height)
    return df


def latest_roster_path() -> Path | None:
    """The most recent roster snapshot, or None if none has landed.

    Ordered by FILENAME, not mtime: the name is the API's `fetched_at`, so it is
    a real ordering key that survives a copy/restore resetting mtimes.
    """
    if not ROSTER_BRONZE_DIR.exists():
        return None
    snaps = sorted(ROSTER_BRONZE_DIR.glob("*.parquet"))
    return snaps[-1] if snaps else None


__all__ = ["ig_roster_raw", "latest_roster_path", "ROSTER_BRONZE_DIR", "DASHBOARD_URL"]
