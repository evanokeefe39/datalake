"""The silver publisher — the ONE Dagster-aware piece of the conform path.

`engine/silver_rt.py` is the pure conform runtime: `conform()` is a function of
bronze, and its import graph is guarded to contain no provider client, no seam
module and no Dagster resource
(`test_zero_api_calls_enforced_structurally`). That guard is what makes "zero
API calls on replay" a structural property rather than a promise.

This module is separate for exactly that reason: an `@asset` needs
`DuckDBResource`, and importing a resource into the runtime would put Dagster
back into conform's transitive imports. Keeping the asset here lets the
runtime stay pure and the guard stay meaningful.

Publishes SEVEN outputs: the six conformed tables plus the quarantine surface.
`silver_rt.register_conformed` writes all seven from one `conform()` call
(``for tid in (*SILVER_TABLES, SILVER_QUARANTINE)``), so they are outputs of one
computation and are declared as such. Before this was a ``@multi_asset``, the
producer was a single opaque asset returning ``None`` and the seven tables were
invisible to the graph — ``is_materializable`` was False for every one of them,
so nothing downstream could be triggered by, or assert against, a specific
table.

The output map is DERIVED from ``silver_rt``'s canonical tuples rather than
hand-written, so adding a table to the runtime cannot silently desync the graph.
"""

import logging

from dagster import AssetKey, AssetOut, AutomationCondition, Nothing, multi_asset

from orchestration.defs.engine import silver_rt as conform
from orchestration.defs.platform import paths as lake
from orchestration.defs.platform.resources import DuckDBResource

logger = logging.getLogger("enrichment.silver")

#: Every table this computation writes: the six conformed tables, then the
#: quarantine surface. Order is canonical (``silver_rt``), so the graph's
#: outputs are a projection of the runtime's own contract.
_PUBLISHED_TABLES: tuple[str, ...] = (*conform.SILVER_TABLES, conform.SILVER_QUARANTINE)

_DESCRIPTIONS: dict[str, str] = {
    conform.SILVER_VISUAL_ANNOTATIONS: (
        "Per-frame visual annotations conformed from bronze_enrichment_raw."
    ),
    conform.SILVER_VISUAL_SUMMARIES: (
        "Per-post visual summaries conformed from bronze_enrichment_raw."
    ),
    conform.SILVER_AUDIO_TRANSCRIPTS: (
        "Audio transcripts conformed from bronze_enrichment_raw."
    ),
    conform.SILVER_TEXT_ANNOTATIONS: (
        "Text annotations conformed from bronze_enrichment_raw."
    ),
    conform.SILVER_TEXT_SUMMARIES: (
        "Per-post text summaries conformed from bronze_enrichment_raw."
    ),
    conform.SILVER_CONTENT_CLASSIFICATION: (
        "Content classification conformed from bronze_enrichment_raw "
        "(gold_analyses's ADR-0011 replacement)."
    ),
    conform.SILVER_QUARANTINE: (
        "LOUD quarantine surface: every terminally-failed row, with its "
        "reason_code and provenance. Rows land here rather than being dropped, "
        "so a failed conform is visible instead of silent."
    ),
}


#: Dagster requires the compute function's return annotation to name one type per
#: declared output. Every output is written out-of-band (Parquet + DuckDB), so
#: each is `Nothing`. Built from the same tuple as `outs` so the two can never
#: disagree — a literal annotation here would be a second place to update, and
#: Dagster fails loudly (not silently) when they drift.
_ReturnAnnotation = tuple[tuple(Nothing for _ in _PUBLISHED_TABLES)]


@multi_asset(
    name="silver_enrichment",
    group_name="enrichment",
    deps=[AssetKey(["bronze_enrichment_raw"])],
    compute_kind="duckdb",
    outs={
        table: AssetOut(
            key=AssetKey([table]),
            description=_DESCRIPTIONS[table],
            group_name="enrichment",
            # Auto-materialize on new bronze: these seven are a pure replay of
            # `bronze_enrichment_raw` with zero provider calls, so running them
            # automatically costs nothing and the pipeline needs no operator.
            # The paid `submit` edge stays unreachable — see the
            # unreachable-submit guard, which fails if a path is introduced.
            automation_condition=AutomationCondition.eager(),
        )
        for table in _PUBLISHED_TABLES
    },
)
def silver_enrichment(duckdb: DuckDBResource) -> _ReturnAnnotation:
    """Deterministic bronze → silver build, orchestrated by Dagster.

    Conforms + validates `bronze_enrichment_raw` into the seven tables above,
    publishing each as an atomic Parquet snapshot and registering them in state
    DuckDB so the gold marts' view SQL compiles. Zero API calls; a re-run is an
    idempotent replay. Returns nothing because Parquet/DuckDB are the outputs —
    the declared `outs` are what make each table addressable in the graph.
    """
    with duckdb.get_connection() as conn:
        conform.conform(
            root=lake.BRONZE_LAKE,
            silver_root=lake.SILVER_LAKE,
            conn=conn,
        )


__all__ = ["silver_enrichment"]
