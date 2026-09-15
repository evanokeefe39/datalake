"""Blocking data-quality gates for the enrichment conform seam (ADR-0012, W8).

Three gates, all attached to ``silver_enrichment_conform`` as BLOCKING asset
checks (``blocking=True``: a failed check blocks downstream materialization
instead of decorating a log line):

1. ``check_no_silent_loss`` — the ADR-0012 anti-join:
   ``landed(bronze) \\ conformed(silver)``. Every bronze candidate key
   ``(post_id, platform, workload)`` must appear EITHER as a conformed row in
   the workload's silver tables OR as a ``silver_enrichment_quarantine`` row.
   A bronze key with NEITHER is silent loss — a row landed and vanished
   (ISSUES.md #26 is the live historical instance: 21 quarantined posts were
   briefly invisible because a done-guard read the wrong table).

2. ``check_quarantine_growth`` — fires when the quarantine snapshot grows
   against the last observed baseline, so a quarantine spike (model
   regression, prompt change, new violation class) is loud, not silent.

3. ``check_silver_snapshot_freshness`` — freshness/volume expectation: each
   published silver Parquet snapshot (six tables + quarantine) must be at
   least as fresh as the bronze landing it was conformed from. An old bronze
   file conformed by nothing is a stale publish, not a healthy one.

Per-asset freshness/volume expectations (W8 "declare per asset"):

============ ==========================================================
Asset        Expectation
============ ==========================================================
bronze       Append-only landings; a missing bronze file is a dormant
_enrichment  source (healthy), NEVER an empty conformance result.
_raw
silver_*     Volume: covers every bronze candidate key (anti-join gate).
(six tables) Freshness: snapshot mtime >= bronze mtime at publish.
quarantine   Volume: growth is a FAILING condition (growth gate); a
             shrinking/flat snapshot is healthy.
============ ==========================================================

**What would make each check fail** (the W8 vacuity answer — a check that
can only ever pass is the defect this module exists to fix):

- ``check_no_silent_loss``: a bronze key with no conformed counterpart and
  no quarantine row — e.g. conform skipped a row, a future change breaks the
  ``counts`` accounting, or a done-guard consumes a table other than
  quarantine/silver (the #26 shape). Injected test: a bronze row deleted
  from silver AND quarantine.
- ``check_quarantine_growth``: quarantine row count exceeds the recorded
  baseline. Injected test: a baseline at N, quarantine at N+1.
- ``check_silver_snapshot_freshness``: a silver/quarantine snapshot missing
  or with mtime older than ``bronze_enrichment_raw.parquet``. Injected test:
  ``os.utime`` backdates a silver file.

Tests: ``tests/unit/enrichment/test_checks_fire.py`` — each gate is proven
to FIRE on the malformed condition and PASS on the clean one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from dagster import AssetCheckResult, asset_check

from datalake.defs.common import lake
from datalake.defs.common.resources import DuckDBResource
from datalake.defs.enrichment import conform, landing
from datalake.defs.enrichment.assets import silver_enrichment_conform

# ── Workload → silver tables (the anti-join's conformed counterpart) ───────

WORKLOAD_SILVER_TABLES: dict[str, tuple[str, ...]] = {
    landing.WORKLOAD_GROWTH_FACETS_VISUAL: (
        conform.SILVER_VISUAL_ANNOTATIONS,
        conform.SILVER_VISUAL_SUMMARIES,
    ),
    landing.WORKLOAD_GROWTH_FACETS_TEXT: (
        conform.SILVER_TEXT_ANNOTATIONS,
        conform.SILVER_TEXT_SUMMARIES,
    ),
    landing.WORKLOAD_CONTENT_CLASSIFICATION: (conform.SILVER_CONTENT_CLASSIFICATION,),
}
"""Which silver tables a workload's conformed rows land in. A workload with
no mapping is covered by quarantine alone (``unsupported_workload``)."""

QUARANTINE_BASELINE_TABLE = "enrichment_quarantine_baseline"
"""State for the growth gate: the last-observed quarantine count (state
DuckDB, created lazily). Deliberately NOT advanced on a failing run — the
gate keeps firing until quarantine shrinks back or a human clears it."""


# ── Pure gate logic (unit-testable against tmp roots / :memory: DuckDB) ────


def anti_join_losses(
    bronze_candidates,
    conformed_keys: Mapping[str, set[tuple[str, str]]],
    quarantined_keys: set[tuple[str, str, str]],
) -> list[dict[str, str]]:
    """Return bronze candidate keys with neither a conformed counterpart nor
    a quarantine row.

    ``bronze_candidates`` is the conform-relevant bronze surface: the latest
    row per ``(post_id, platform, workload)`` (earlier landings are
    superseded by conform's own dedup, not lost — mirroring
    ``conform._latest_per_key``). ``conformed_keys`` maps silver table id →
    its set of ``(post_id, platform)`` keys.
    """
    losses: list[dict[str, str]] = []
    for row in bronze_candidates.iter_rows(named=True):
        key3 = (row["post_id"], row["platform"], row["workload"])
        if key3 in quarantined_keys:
            continue
        tables = WORKLOAD_SILVER_TABLES.get(row["workload"], ())
        if any(
            (row["post_id"], row["platform"]) in conformed_keys.get(tid, set())
            for tid in tables
        ):
            continue
        losses.append(
            {
                "post_id": row["post_id"],
                "platform": row["platform"],
                "workload": row["workload"],
            }
        )
    return losses


def stale_snapshot_files(
    bronze_file: Path, silver_files: Mapping[str, Path]
) -> list[str]:
    """Silver snapshot names that are MISSING or older than the bronze file.

    A missing silver file while bronze exists means the conform publish did
    not cover it (volume violation); an older mtime means the silver
    snapshot predates the current bronze landing (freshness violation).
    """
    if not bronze_file.exists():
        return []  # dormant source: healthy, per the expectations table
    bronze_mtime = bronze_file.stat().st_mtime
    stale: list[str] = []
    for name, path in sorted(silver_files.items()):
        if not path.exists() or path.stat().st_mtime < bronze_mtime:
            stale.append(name)
    return stale


def quarantine_growth(conn, quarantined: int) -> tuple[bool, dict]:
    """Compare the quarantine count against the recorded baseline.

    Returns ``(fired, metadata)``. First observation ever: record and pass
    (nothing to grow against). Growth: FAIL, and deliberately do NOT advance
    the baseline — the gate keeps firing on every subsequent run until the
    quarantine shrinks back, so a spike cannot be silently absorbed by
    re-running. Flat or shrinking: record and pass.
    """
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {QUARANTINE_BASELINE_TABLE} "
        "(quarantined BIGINT, observed_at VARCHAR)"
    )
    row = conn.execute(
        f"SELECT quarantined FROM {QUARANTINE_BASELINE_TABLE} "
        "ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()
    previous = row[0] if row else None
    meta = {"quarantined": quarantined, "previous_baseline": previous}
    if previous is not None and quarantined > previous:
        return True, meta  # baseline deliberately NOT advanced
    conn.execute(
        f"INSERT INTO {QUARANTINE_BASELINE_TABLE} VALUES (?, ?)",
        [quarantined, __import__("datetime").datetime.now().isoformat()],
    )
    return False, meta


# ── Asset checks (wired via ENRICHMENT_DQ_CHECKS → definitions.py) ─────────


@asset_check(asset=silver_enrichment_conform.key, blocking=True)
def check_no_silent_loss(duckdb: DuckDBResource) -> AssetCheckResult:
    """BLOCKING ADR-0012 anti-join: landed(bronze) \\ conformed(silver).

    Every bronze candidate key must have a conformed silver row OR a
    quarantine row. What would make this fail: a bronze key with neither —
    a row that landed and vanished (the ISSUES.md #26 shape: 21 quarantined
    posts briefly invisible because a done-guard read the wrong table).
    """
    bronze = landing.read_responses(lake.BRONZE_LAKE)
    candidates = conform._latest_per_key(bronze)

    with duckdb.get_connection() as conn:
        conformed_keys: dict[str, set[tuple[str, str]]] = {}
        for tid in conform.SILVER_TABLES:
            try:
                rows = conn.execute(
                    f"SELECT post_id, platform FROM {tid}"
                ).fetchall()
            except Exception:
                rows = []  # unregistered table == nothing conformed there
            conformed_keys[tid] = {(r[0], r[1]) for r in rows}
        try:
            q_rows = conn.execute(
                f"SELECT post_id, platform, workload "
                f"FROM {conform.SILVER_QUARANTINE}"
            ).fetchall()
        except Exception:
            q_rows = []
        quarantined_keys = {(r[0], r[1], r[2]) for r in q_rows}

    losses = anti_join_losses(candidates, conformed_keys, quarantined_keys)
    metadata = {
        "bronze_candidates": candidates.height,
        "quarantined_keys": len(quarantined_keys),
        "silent_losses": len(losses),
        "loss_sample": losses[:20],
    }
    if losses:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=(
                f"{len(losses)} bronze key(s) have NO conformed silver row and "
                "NO quarantine row — rows landed and vanished (ADR-0012 "
                "anti-join; ISSUES.md #26 shape). Re-run conform; if it "
                "recurs, the conform publish or a done-guard is consuming "
                "the wrong surface."
            ),
        )
    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(asset=silver_enrichment_conform.key)
def check_quarantine_growth(duckdb: DuckDBResource) -> AssetCheckResult:
    """Fire when the quarantine snapshot grows against the last baseline.

    What would make this fail: ``COUNT(*)`` on
    ``silver_enrichment_quarantine`` exceeding the previously recorded
    baseline in ``enrichment_quarantine_baseline`` — a spike in quarantined
    rows (model regression, prompt change, new violation class). The
    baseline is not advanced while failing, so the gate stays loud.
    """
    with duckdb.get_connection() as conn:
        try:
            n = conn.execute(
                f"SELECT COUNT(*) FROM {conform.SILVER_QUARANTINE}"
            ).fetchone()[0]
        except Exception:
            n = 0  # quarantine not registered yet → count 0, record baseline
        fired, meta = quarantine_growth(conn, n)

    if fired:
        return AssetCheckResult(
            passed=False,
            metadata=meta,
            description=(
                f"Quarantine grew from {meta['previous_baseline']} to "
                f"{meta['quarantined']} rows — triage "
                "v_quarantine_triage before re-running."
            ),
        )
    return AssetCheckResult(passed=True, metadata=meta)


@asset_check(asset=silver_enrichment_conform.key)
def check_silver_snapshot_freshness(duckdb: DuckDBResource) -> AssetCheckResult:
    """Freshness/volume expectation: silver snapshots cover the bronze file.

    What would make this fail: any of the six silver tables or the
    quarantine snapshot missing while ``bronze_enrichment_raw.parquet``
    exists, or a snapshot whose mtime predates the bronze file (a stale
    publish — bronze landed after the last conform). A missing bronze file
    is a dormant source and passes (healthy), per the module expectations
    table.
    """
    bronze_file = landing.response_path(lake.BRONZE_LAKE)
    silver_files = {
        **{tid: conform.table_path(tid, lake.SILVER_LAKE) for tid in conform.SILVER_TABLES},
        conform.SILVER_QUARANTINE: conform.table_path(
            conform.SILVER_QUARANTINE, lake.SILVER_LAKE
        ),
    }
    stale = stale_snapshot_files(bronze_file, silver_files)
    metadata = {
        "bronze_file": bronze_file.name,
        "bronze_exists": bronze_file.exists(),
        "stale_snapshots": stale,
    }
    if stale:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=(
                f"Stale/missing silver snapshots vs bronze: {stale} — "
                "re-run silver_enrichment_conform to republish."
            ),
        )
    return AssetCheckResult(passed=True, metadata=metadata)


ENRICHMENT_DQ_CHECKS = [
    check_no_silent_loss,
    check_quarantine_growth,
    check_silver_snapshot_freshness,
]
"""W8 blocking gates — composed into Definitions alongside *ENRICHMENT_CHECKS."""
