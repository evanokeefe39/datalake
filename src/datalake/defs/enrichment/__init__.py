"""Enrichment architecture — the ADR-0011 layered model.

Bronze lands provider responses verbatim (`bronze_enrichment_raw`); silver
conforms + validates deterministically; gold marts compose the canonical
views. Orchestration state is Dagster-native (ADR-0012/0013): the retired
ops.sqlite queue (`batch_jobs`/`batch_items`/`dead_letter`) has no read or
write on any live path. The loop: drain → submit → harvest (ADR-0014).
"""

from .assets import (
    ENRICHMENT_CHECKS,
    silver_enrichment_conform,
)
from .checks import ENRICHMENT_DQ_CHECKS
from .harvest import (
    enrichment_harvest_job,
    harvest_enrichment_op,
    harvest_pending,
    mint_retries,
    report_harvested,
)
from .submit import (
    build_items,
    discover_pending,
    enrichment_submit_job,
    submit_enrichment_op,
    submit_pending,
)

__all__ = [
    # Assets
    "silver_enrichment_conform",
    "ENRICHMENT_CHECKS",
    "ENRICHMENT_DQ_CHECKS",
    # Submit (ADR-0014: partition-discovered, seam-mediated)
    "discover_pending",
    "build_items",
    "submit_pending",
    "submit_enrichment_op",
    "enrichment_submit_job",
    # Harvest (ADR-0014 D2/D3: harvested producer + retry driver)
    "harvest_pending",
    "report_harvested",
    "mint_retries",
    "harvest_enrichment_op",
    "enrichment_harvest_job",
    # Prompt/version registry
    "register_current_prompt",
    "resolve_prompt",
]
