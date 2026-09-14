"""Domain dispatch tables for enrichment — the slim survivor of the
retired queue-path worker (ADR-0012).

RETIRED and DELETED from this module (W-FREEZE, ADR-0014): the Gemini
BATCH request builder (``build_requests_for_items``), the retired
``gold_analyses`` upsert (``write_gold`` — the old write path is FROZEN,
and the target store is ``silver_content_classification``), the
dead-letter insert, the attempt/backoff/quota helpers, and
``_resolve_media_for_post`` (a Gemini File-API transport — provider
transports belong behind the seam adapters). The queue primitives they
called are gone from ``batch.py``.

What remains:
- the domain dispatch tables (``_SILVER_TABLES`` — the single source of
  truth for platform → silver table, consumed by ``media_upload.py`` and
  the partition-discovered submit stage),
- ``_now_iso`` (shared timestamp helper, consumed by ``facets.py``).
"""

from __future__ import annotations

from datalake.defs.common.resources import SQLiteResource  # noqa: F401 (re-export shape)
from datalake.defs.enrichment.batch import _now_iso  # noqa: F401

# ── Domain dispatch tables ───────────────────────────────────────────────────

_SILVER_TABLES: dict[str, str] = {
    "instagram": "silver_ig_posts",
}
