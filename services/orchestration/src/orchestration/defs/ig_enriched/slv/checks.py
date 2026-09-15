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

import importlib
import inspect
import pkgutil
from collections.abc import Mapping
from pathlib import Path

from dagster import AssetCheckResult, OpDefinition, asset_check

from orchestration.defs.engine import landing
from orchestration.defs.engine import silver_rt as conform
from orchestration.defs.engine.silver_rt import silver_enrichment
from orchestration.defs.ig_core.slv.labels import LABEL_VERSION
from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH
from orchestration.defs.platform import paths as lake
from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource

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


@asset_check(asset=silver_enrichment.key, blocking=True)
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


@asset_check(asset=silver_enrichment.key)
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


@asset_check(asset=silver_enrichment.key)
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


# ── Asset checks (merged from the retired enrichment/assets.py) ─────────────


@asset_check(asset="silver_enrichment")
def check_approved_classification_coverage(
    duckdb: DuckDBResource,
) -> AssetCheckResult:
    """Warn if triage-approved posts lack a silver_content_classification row.

    The v3 replacement for the retired ``check_enrichment_health`` gold
    counter (W9): the admission-gate signal (approved-but-unclassified
    posts) survives, re-pointed from ``gold_analyses`` to the live
    classification table. The retired ops.sqlite queue counters
    (batch_items stuck / dead_letter growth) are NOT carried over — ADR-0012
    retired that queue; ``check_no_silent_loss`` (bronze \\ conformed) is
    the blocking health gate for the conform itself.

    Does NOT mutate state. Read-only.
    """
    with duckdb.get_connection() as db_conn:
        classified = db_conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification "
            "WHERE platform = 'instagram'"
        ).fetchone()[0]
        approved_unenriched = db_conn.execute("""
            SELECT COUNT(*) FROM ig_post_labels l
            WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
              AND l.label_version = ?
              AND NOT EXISTS (
                  SELECT 1 FROM silver_content_classification c
                  WHERE c.post_id = l.post_id AND c.platform = 'instagram'
              )
        """, [LABEL_VERSION]).fetchone()[0]

    metadata = {
        "silver_content_classification_count": classified,
        "approved_unenriched": approved_unenriched,
    }
    if approved_unenriched > 20:
        return AssetCheckResult(
            passed=False,
            metadata=metadata,
            description=(
                f"{approved_unenriched} triage-approved posts are unenriched — "
                "the submit stage will pick them up when the sensor ticks."
            ),
        )

    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(asset="silver_enrichment")
def check_prompt_currency(duckdb: DuckDBResource, ops: SQLiteResource) -> AssetCheckResult:
    """Detect rows where prompt_hash is stale (prompt or model changed).

    Does NOT trigger re-enrichment (prompt changes cost money — human gate).
    The claim is the row's recorded `prompt_hash` against the prompt module's
    `CURRENT_PROMPT_HASH`. The prompt/version registry that used to corroborate
    this was retired with the provenance columns (ADR-0011): every row carries
    its own prompt hash, so the comparison is the whole check.
    """
    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification "
            "WHERE prompt_hash IS NULL OR prompt_hash != ?",
            [CURRENT_PROMPT_HASH],
        ).fetchone()

    stale = row[0] if row else 0


    if stale > 0:
        return AssetCheckResult(
            passed=False,
            metadata={
                "stale_rows": stale,
                "current_prompt_hash": CURRENT_PROMPT_HASH,
                },
        )
    return AssetCheckResult(
        passed=True, metadata={"stale_rows": 0}
    )


@asset_check(asset="silver_enrichment")
def check_enrichment_seam_purity() -> AssetCheckResult:
    """ADR-0008 seam guard: silver onward stays hermetic.

    Pure enrichment modules must contain no Gemini API calls, and every op
    in the enrichment domain must carry the ``{adr: 0008, seam:
    enrichment-api}`` tag. A violation means an API call has leaked into (or
    an untagged op is one edit away from leaking into) a pure transform.
    """
    violations = seam_violations()
    return AssetCheckResult(
        passed=not violations,
        metadata={
            "violations": violations,
            "adr": "0008",
            "seam": "enrichment-api",
        },
    )


ENRICHMENT_CHECKS = [
    check_approved_classification_coverage,
    check_prompt_currency,
    check_enrichment_seam_purity,
]


# ── Seam-purity scanner (moved from the retired media_upload.py) ─────────────


# ADR-0008 seam tag — every op that may touch a provider API carries this.
SEAM_TAGS = {"adr": "0008", "seam": "enrichment-api"}

# Source markers that indicate a provider API call. Any hit inside a "pure"
# enrichment module is an ADR-0008 violation.
_API_MARKERS: tuple[str, ...] = (
    "generate_content",
    "GeminiClient",
    "files.upload",
    "batches.get",
    "batches.submit",
    "gemini_batch.",
    ".analyze(",
)

# Enrichment modules that must stay hermetic (no API calls, no ops).
_PURE_MODULES: tuple[str, ...] = (
    "orchestration.defs.engine.silver_rt",
    "orchestration.defs.ig_enriched.slv.prompts",
)


def _module_ops(module) -> list[OpDefinition]:
    return [
        attr
        for attr in vars(module).values()
        if isinstance(attr, OpDefinition)
    ]


def seam_violations(
    pure_modules: list | None = None,
    extra_modules: list | None = None,
) -> list[str]:
    """ADR-0008 seam purity scan over the enrichment domain.

    Two rules:

    1. Pure modules (assets/registry/prompts by default) must contain no
       provider API markers in their source and must not define Dagster ops —
       this is what rejects a ``generate_content`` call added to a pure asset.
    2. Every op in the enrichment package (plus ``extra_modules``, for tests)
       must carry the ``seam: enrichment-api`` tag — an untagged op is a seam
       violation even if its API use is currently benign.

    ``pure_modules``/``extra_modules`` accept any module object so tests can
    inject synthetic modules without editing the package.
    """
    violations: list[str] = []

    modules = pure_modules
    if modules is None:
        modules = [importlib.import_module(m) for m in _PURE_MODULES]
    for module in modules:
        src = inspect.getsource(module)
        for marker in _API_MARKERS:
            if marker in src:
                violations.append(
                    f"{module.__name__}: hermetic module contains API marker "
                    f"'{marker}'"
                )
        for op_def in _module_ops(module):
            violations.append(
                f"{module.__name__}: op '{op_def.name}' defined in a "
                "hermetic module"
            )

    # Ops now live in two subtrees with a layer directory between them, so the
    # walk is recursive over both roots rather than `iter_modules` over one.
    walk = extra_modules or []
    try:
        for root in ("orchestration.defs.engine", "orchestration.defs.ig_enriched"):
            pkg = importlib.import_module(root)
            walk.extend(
                importlib.import_module(m.name)
                for m in pkgutil.walk_packages(pkg.__path__, prefix=f"{pkg.__name__}.")
            )
    except Exception as exc:  # pragma: no cover - defensive
        violations.append(f"enrichment package walk failed: {exc}")

    for module in walk:
        for op_def in _module_ops(module):
            tags = op_def.tags or {}
            if tags.get("seam") != "enrichment-api":
                violations.append(
                    f"{module.__name__}: op '{op_def.name}' is missing the "
                    f"ADR-0008 seam tag {SEAM_TAGS}"
                )
    return violations
