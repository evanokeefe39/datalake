"""Batch-native growth-facets enrichment — harvest into bronze, conform to silver.

Runs on the standalone **qwen-batch service** (reached ONLY through the
seam's ``service_backed`` adapter — see ``adapters.py``), NOT the
Gemini BATCH API. The IG-gold enrichment flow (``analysis.py`` via the
direct-batch module) is a separate target and still uses Gemini.

Flow (one-shot CLI driver, ``scripts/enrich_facets_batch.py``):

    enumerate_targets        (posts lacking a current conformed silver row
                              under the qwen-scoped engine model)
    → build_facets_batch_requests
        visual: media URLs resolved to scrape-time cached local paths
          (media_paths.media_urls_to_local_paths — the SERVICE frame-samples
          video files with ffmpeg on its own host; the client only hands it
          absolute paths). Text mode sends images=[] (caption only).
    → submit_facets_batch    (the LOUD /health gate — US-EENG-2 — runs at
          the adapter boundary, then ONE qwen service job via
          ``seam.build_adapter("service_backed")`` with max_tokens/mode in
          ``JobSpec``; the returned service job id IS the in-flight record
          — ADR-0013: NO ledger)
    → wait_for_facets_batches (poll ``GET /jobs/{id}`` via the seam adapter
          to terminal, bounded)
    → harvest_facets_batches (retrieve via the seam → land VERBATIM into
          ``bronze_enrichment_raw`` (landing.py) BEFORE any parsing → parse
          → validate + count loudly. The typed silver tables are published
          by conform from bronze — the retired ``gold_growth_facets``
          write path was removed 2026-09-15 (W9).)

Job state lives in the qwen-batch-service's own store (``GET /jobs/{id}``,
read through the seam's service-backed adapter). This module neither creates
nor consults any ``ops.sqlite`` ledger (ADR-0013); re-harvesting a job is
idempotent because the bronze landing keys on the service job id.

Two passes (US-EFAC-3/4):

- **visual** — ``build_growth_facets_prompt(caption, n_media)`` + image
  paths; parse with ``parse_universal_response``.
- **text** — ``build_text_facets_prompt`` + NO media; parse with
  ``parse_text_response``.
"""

from __future__ import annotations

import logging
import os
import time

from orchestration.defs.engine import provider as seam
from orchestration.defs.engine import service_backed
from orchestration.defs.engine.landing import (
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    land_response,
)
from orchestration.defs.ig_enriched.slv import text as text_mod
from orchestration.defs.ig_enriched.slv import visual
from orchestration.defs.ig_enriched.slv.prompts import (
    _DEFAULT_QWEN_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
)
from orchestration.defs.ig_enriched.slv.schemas import (
    GROWTH_FACETS_SCHEMA_VERSION,
)
from orchestration.defs.ig_enriched.slv.workloads import (
    _n_media_for,
)

logger = logging.getLogger("enrichment.facets_batch")

# Qwen list prices (USD per 1M tokens) — advisory cost projection only,
# not billing. Env-overridable for price drift.
QWEN_INPUT_PRICE_PER_M = float(os.environ.get("QWEN_INPUT_PRICE_PER_M", "0.03"))
QWEN_OUTPUT_PRICE_PER_M = float(os.environ.get("QWEN_OUTPUT_PRICE_PER_M", "0.13"))

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = visual.UNIVERSAL_MAX_OUTPUT_TOKENS
_EST_OUTPUT_TOKENS_TEXT = 1024

# Advisory per-media input-token estimates for the qwen service: images are
# vision tokens; videos are frame-sampled server-side into 8 frames.
_TOKENS_PER_IMAGE = 258
_VIDEO_FRAMES = 8

_TEXT_REQUIRED = (
    "hook_content", "hook_type", "is_sponsored", "sponsorship_signal",
    "claimed_results", "cta_type", "audience_named", "value_depth",
    "replicable_tactic", "evidence", "brand_safety",
)


# ── Target enumeration ───────────────────────────────────────────────────────


_MODE_WORKLOAD = {
    "visual": WORKLOAD_GROWTH_FACETS_VISUAL,
    "text": WORKLOAD_GROWTH_FACETS_TEXT,
}
"""Each pass is a DIFFERENT bronze workload — one submit per pass, whose
responses land under that pass's workload (never one submit per table)."""

_MODE_PROMPT_HASH = {
    "visual": CURRENT_FACETS_PROMPT_HASH,
    "text": CURRENT_TEXT_FACETS_PROMPT_HASH,
}


def _service_adapter(
    base_url: str | None, model: str
) -> seam.ProviderAdapter:
    """The service-backed adapter from the seam registry.

    ``seam.build_adapter`` is the only place a provider is named. Job-state
    reads (poll/retrieve) go to the service's own job store over HTTP —
    never to ``ops.sqlite`` (ADR-0013).
    """
    return seam.build_adapter(service_backed.PROVIDER_NAME, base_url=base_url, model=model)


def submit_facets_batch(
    items: list[dict],
    mode: str,
    model: str = _DEFAULT_QWEN_MODEL,
    base_url: str | None = None,
) -> str:
    """Submit ONE qwen service job for a facet pass; return the job id.

    Preconditions: `items` non-empty (built by ``build_facets_batch_requests``),
    `mode` in ``("visual", "text")``. US-EENG-2: the health gate runs LOUDLY
    first — a down service raises, never a quiet "nothing to do".

    ADR-0013: NO placeholder or ledger row is written. The returned service
    job id IS the in-flight record; the service owns the job store and the
    caller (Dagster partition set or the operator) carries the id until
    harvest. ``max_tokens`` and ``mode`` travel in ``JobSpec`` at submit
    time — no constructor kwargs; job-state reads go through the seam
    (``wait_for_facets_batches`` / ``harvest_facets_batches``).
    """
    if not items:
        raise ValueError("items must not be empty")
    if mode not in _MODE_WORKLOAD:
        raise ValueError(f"unknown mode: {mode}")
    adapter = _service_adapter(base_url, model)
    max_tokens = (
        visual.UNIVERSAL_MAX_OUTPUT_TOKENS if mode == "visual" else 1024
    )
    job_id = adapter.submit(
        [
            seam.Item(
                custom_key=r["custom_key"],
                prompt=r["prompt"],
                images=tuple(r.get("images") or []),
            )
            for r in items
        ],
        job_spec=seam.JobSpec(max_tokens=max_tokens, mode=mode),
    )
    logger.info(
        "Facets batch (%s): %d items → qwen job %s", mode, len(items), job_id
    )
    return job_id


def wait_for_facets_batches(
    job_ids: list[str],
    poll_seconds: int = 60,
    timeout_seconds: int = 24 * 3600,
    base_url: str | None = None,
    model: str = _DEFAULT_QWEN_MODEL,
) -> dict[str, str]:
    """Poll service job ids until each reaches a terminal state (bounded).

    Job state is read from the SERVICE (``GET /jobs/{id}`` via the seam
    adapter) — ADR-0013: there is no ledger to consult. Returns
    ``{job_id: state}`` in the seam's canonical vocabulary. An unknown job id
    surfaces as the service's error (loud, never a quiet skip). Raises
    ``RuntimeError`` on timeout.
    """
    adapter = _service_adapter(base_url, model)
    results: dict[str, str] = {}
    remaining = set(job_ids)
    deadline = time.monotonic() + timeout_seconds
    while remaining:
        for job_id in list(remaining):
            state = adapter.normalize_state(adapter.poll(job_id))
            if adapter.is_terminal(state):
                results[job_id] = state
                remaining.discard(job_id)
        if remaining and time.monotonic() > deadline:
            raise RuntimeError(
                f"Timed out waiting for facet jobs: {sorted(remaining)}"
            )
        if remaining:
            time.sleep(poll_seconds)
    return results


def harvest_facets_batches(
    conn,
    job_ids: list[str],
    mode: str,
    model: str = _DEFAULT_QWEN_MODEL,
    base_url: str | None = None,
    root: str | os.PathLike[str] | None = None,
) -> dict:
    """Retrieve terminal facet jobs; land VERBATIM, then parse and count.

    ADR-0011/0013 sequencing per retrieved item: the provider response is
    landed VERBATIM into ``bronze_enrichment_raw`` — under this pass's
    workload (visual ≠ text), with ``run_id`` = the service job id, which
    makes re-harvesting the same job idempotent on the natural key — BEFORE
    any parsing. The response is then parsed + validated so invalid payloads
    are counted loudly (``invalid``); nothing is written to DuckDB here —
    conform publishes the typed silver tables from bronze. Failures land
    with ``ok=False`` and a populated ``error_message``; failure is READ
    from bronze, never inferred from a missing conformed row.

    Job state is read from the service via the seam adapter (ADR-0013 — no
    ledger); the caller supplies the in-flight job ids. Non-terminal jobs are
    skipped (counted in ``skipped``); a terminally-failed job is logged
    loudly and counted in ``failed_jobs`` (it has no results to land).
    Per-item failures are landed AND counted, never silently dropped — their
    targets are NOT recorded as done, so the driver re-discovers them.
    """
    workload = _MODE_WORKLOAD.get(mode)
    if workload is None:
        raise ValueError(f"unknown mode: {mode}")
    if not job_ids:
        raise ValueError(
            "no job ids to harvest — there is no ledger (ADR-0013); the "
            "caller supplies the in-flight set (the job ids returned by "
            "submit_facets_batch, or Dagster's partition state)"
        )
    prompt_hash = _MODE_PROMPT_HASH[mode]
    adapter = _service_adapter(base_url, model)
    written = invalid = failed_items = failed_jobs = skipped = landed = 0
    for job_id in job_ids:
        state = adapter.normalize_state(adapter.poll(job_id))
        if not adapter.is_terminal(state):
            logger.info("Facets job %s not yet terminal (%s)", job_id, state)
            skipped += 1
            continue
        if state != seam.COMPLETED:
            logger.error("Facets job %s terminal state %s", job_id, state)
            failed_jobs += 1
            continue
        for res in adapter.retrieve(job_id):
            post_id = res.custom_key
            text = res.response_text
            ok = res.ok and text is not None
            error_message = res.error or (
                None if ok else "service returned no output body"
            )
            res_model = res.model or model
            # VERBATIM FIRST — the paid/stochastic part ends here; parsing
            # below must never gate what bronze records.
            landed += land_response(
                post_id=post_id,
                platform="instagram",
                workload=workload,
                # provider names the PROVIDER, not the transport adapter:
                # res.provider carries the adapter name ('service_backed'),
                # which the landing KNOWN_PROVIDERS guard rightly refuses.
                # Workload->provider is settled — this path IS qwen.
                provider="qwen",
                model=res_model,
                prompt_hash=prompt_hash,
                schema_version=GROWTH_FACETS_SCHEMA_VERSION,
                run_id=job_id,
                response_text=text if text is not None else "",
                ok=ok,
                error_message=error_message,
                root=root,
            )
            if not ok:
                logger.warning(
                    "Post %s service item error: %s", post_id, error_message
                )
                failed_items += 1
                continue
            if mode == "text":
                parsed = text_mod.parse_text_response(text)
            else:
                n_media = _n_media_for(conn, post_id)
                parsed = visual.parse_universal_response(text, n_media)
            if parsed["errors"] or (
                parsed["text_facets"] is None if mode == "text"
                else parsed["visual_facets"] is None
            ):
                logger.warning(
                    "Post %s %s facets invalid: %s",
                    post_id, mode, parsed["errors"],
                )
                invalid += 1
                continue
            written += 1
    return {
        "written": written,
        "invalid": invalid,
        "failed_items": failed_items,
        "failed_jobs": failed_jobs,
        "skipped": skipped,
        "landed": landed,
        "jobs": len(job_ids),
    }
