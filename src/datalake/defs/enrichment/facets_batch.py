"""Batch-native growth-facets enrichment — facets → ``gold_growth_facets``.

Runs on the standalone **qwen-batch service** (``qwen_client.py``), NOT the
Gemini BATCH API. The IG-gold enrichment flow (``analysis.py`` /
``gemini_batch.py``) is a separate target and still uses Gemini.

Flow (one-shot CLI driver, ``scripts/enrich_facets_batch.py``):

    enumerate_targets        (posts lacking a current gold row under the
                              qwen-scoped CURRENT_*_PROMPT_HASH)
    → build_facets_batch_requests
        visual: media URLs resolved to scrape-time cached local paths
          (media_paths.media_urls_to_local_paths — the SERVICE frame-samples
          video files with ffmpeg on its own host; the client only hands it
          absolute paths). Text mode sends images=[] (caption only).
    → submit_facets_batch    (check_health LOUDLY first — US-EENG-2 — then
          ONE qwen service job via qwen_client.submit_job; the returned
          service job id IS the in-flight record — ADR-0013: NO ledger)
    → wait_for_facets_batches (poll ``GET /jobs/{id}`` via the seam adapter
          to terminal, bounded)
    → harvest_facets_batches (retrieve via the seam → land VERBATIM into
          ``bronze_enrichment_raw`` (landing.py) BEFORE any parsing → parse
          → validate → facets.write_gold_facets_pass_conn MERGE upsert)

Job state lives in the qwen-batch-service's own store (``GET /jobs/{id}``,
read through the seam's service-backed adapter). This module neither creates
nor consults any ``ops.sqlite`` ledger (ADR-0013); re-harvesting a job is
idempotent because the bronze landing keys on the service job id.

Two passes, partial-storage contract (US-EFAC-3/4):

- **visual** — ``build_growth_facets_prompt(caption, n_media)`` + image
  paths; parse with ``parse_universal_response``; writes visual sub-fields +
  content_summary / image_summaries.
- **text** — ``build_text_facets_prompt`` + NO media; parse with
  ``parse_text_response``; merges text sub-fields.

Each write merges into the row's existing ``growth_facets_json`` so the
"""

from __future__ import annotations

import json
import logging
import os
import time

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment import facets, qwen_client, seam
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
    VISUAL_FACET_FIELDS,
)
from datalake.defs.enrichment.landing import (
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    land_response,
)
from datalake.defs.enrichment.media_paths import (
    is_video_path,
    media_urls_to_local_paths,
)
from datalake.defs.enrichment.prompts import (
    _DEFAULT_QWEN_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    build_growth_facets_prompt,
    build_text_facets_prompt,
)

logger = logging.getLogger("enrichment.facets_batch")

# Qwen list prices (USD per 1M tokens) — advisory cost projection only,
# not billing. Env-overridable for price drift.
QWEN_INPUT_PRICE_PER_M = float(os.environ.get("QWEN_INPUT_PRICE_PER_M", "0.03"))
QWEN_OUTPUT_PRICE_PER_M = float(os.environ.get("QWEN_OUTPUT_PRICE_PER_M", "0.13"))

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = facets.UNIVERSAL_MAX_OUTPUT_TOKENS
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


def enumerate_targets(
    conn,
    mode: str,
    limit: int | None = None,
    post_ids: list[str] | None = None,
) -> list[dict]:
    """Posts eligible for the given pass, not yet done under its prompt hash.

    visual: media-bearing, non-empty caption, no gold row under
    ``CURRENT_FACETS_PROMPT_HASH``.
    text: non-empty caption, no stored row carrying the full text-layer
    sub-schema (row-union detection — robust across the visual pass's
    prompt_hash column ownership).
    """
    if mode not in ("visual", "text"):
        raise ValueError(f"unknown mode: {mode}")
    where = "TRIM(caption) <> ''"
    params: list = []
    if mode == "visual":
        where += " AND media_files IS NOT NULL AND media_files <> '[]'"
    if post_ids:
        where += " AND post_id IN (" + ",".join("?" * len(post_ids)) + ")"
        params.extend(post_ids)
    # Idempotent ensure: plan mode may run against a state db that has never
    # materialized gold_growth_facets.
    conn.execute(facets._GOLD_FACETS_DDL)
    rows = conn.execute(
        f"SELECT post_id, caption, media_files FROM silver_ig_posts "
        f"WHERE {where} ORDER BY post_id",
        params,
    ).fetchall()
    done = _done_post_ids(conn, mode)
    targets = [
        {"post_id": r[0], "caption": r[1] or "", "media_files": r[2]}
        for r in rows
        if r[0] not in done
    ]
    return targets[:limit] if limit else targets


def _done_post_ids(conn, mode: str) -> set[str]:
    """Post_ids already fully enriched for ``mode`` under the current engine
    (model) + schema — content-based, so a pass-owned prompt_hash overwrite
    cannot double-spend, while rows from a superseded engine (e.g. gemini-era)
    are re-enqueued for the current (qwen) engine."""
    if mode == "visual":
        # Done iff schema_version is current, the stored facet JSON carries
        # every required visual field, AND the row was produced by the current
        # engine (model). Must NOT gate on prompt_hash: the text pass overwrites
        # prompt_hash on the same row, so a hash-only check would re-enqueue
        # (and re-pay for) text-done posts every run. model is stable across
        # both passes (each stamps the current qwen model), so it stays a valid
        # discriminator that also re-enqueues gemini-era rows under qwen.
        rows = conn.execute(
            "SELECT post_id, growth_facets_json, schema_version, model "
            "FROM gold_growth_facets"
        ).fetchall()
        done = set()
        for post_id, blob, schema_version, model in rows:
            if model != _DEFAULT_QWEN_MODEL:
                continue
            if schema_version != GROWTH_FACETS_SCHEMA_VERSION:
                continue
            try:
                payload = json.loads(blob) if blob else None
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and all(
                f in payload for f in VISUAL_FACET_FIELDS
            ):
                done.add(post_id)
        return done
    # text: done iff the stored JSON carries every required text field AND the
    # row was produced by the current engine (model) — mirroring visual.
    rows = conn.execute(
        "SELECT post_id, growth_facets_json, model FROM gold_growth_facets "
        "WHERE model = ?",
        [_DEFAULT_QWEN_MODEL],
    ).fetchall()
    done = set()
    for post_id, blob, model in rows:
        try:
            payload = json.loads(blob) if blob else None
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and all(f in payload for f in _TEXT_REQUIRED):
            done.add(post_id)
    return done

# ── Request building ─────────────────────────────────────────────────────────


def build_facets_batch_requests(
    ops: SQLiteResource,
    conn,
    targets: list[dict],
    mode: str,
) -> list[dict]:
    """Build qwen service job items for facet targets.

    Visual targets resolve media URLs to scrape-time cached LOCAL file paths
    (``media_urls_to_local_paths``) at submit time — the service reads files
    from its own host disk and frame-samples videos with ffmpeg. Text targets
    send ``images=[]`` (caption only). ``custom_key`` is the post_id so
    harvest maps responses directly.

    A visual target whose media resolves to NO cached path is skipped with a
    logged reason (deterministic, re-discovered on the next run) — see the
    module docstring.
    """
    items: list[dict] = []
    for t in targets:
        post_id = t["post_id"]
        caption = t["caption"]
        if mode == "visual":
            images = media_urls_to_local_paths(
                ops, t["media_files"], include_video=True
            )
            if not images:
                logger.warning(
                    "Post %s: no cached media paths resolvable — skipped "
                    "(re-discovered on the next run after media cache fill)",
                    post_id,
                )
                continue
            req = {
                "custom_key": post_id,
                "prompt": build_growth_facets_prompt(caption, len(images)),
                "images": images,
            }
        else:
            req = {
                "custom_key": post_id,
                "prompt": build_text_facets_prompt(caption),
                "images": [],
            }
        items.append(req)
    return items


# ── Cost projection ──────────────────────────────────────────────────────────


def estimate_facets_cost(items: list[dict]) -> tuple[int, float]:
    """(estimated input tokens, projected USD) for a qwen item list.

    Advisory only (not billing): prompt chars/4 + vision tokens per image
    (videos counted as their server-side frame sample), priced at the qwen
    list rates.
    """
    input_tokens = 0
    out_tokens = 0
    for r in items:
        n_video = sum(1 for p in r.get("images") or [] if is_video_path(p))
        n_img = len(r.get("images") or []) - n_video
        input_tokens += (
            len(r.get("prompt") or "") // 4
            + n_img * _TOKENS_PER_IMAGE
            + n_video * _VIDEO_FRAMES * _TOKENS_PER_IMAGE
        )
        out_tokens += (
            _EST_OUTPUT_TOKENS_VISUAL if r.get("images") else _EST_OUTPUT_TOKENS_TEXT
        )
    cost = (
        input_tokens / 1_000_000 * QWEN_INPUT_PRICE_PER_M
        + out_tokens / 1_000_000 * QWEN_OUTPUT_PRICE_PER_M
    )
    return input_tokens, cost


# ── Submit / wait / harvest ──────────────────────────────────────────────────


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
    return seam.build_adapter("service_backed", base_url=base_url, model=model)


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
    harvest. ``max_tokens`` is per-mode, so submit stays on the HTTP client
    (the seam adapter's contract is model-only); job-state reads go through
    the seam (``wait_for_facets_batches`` / ``harvest_facets_batches``).
    """
    if not items:
        raise ValueError("items must not be empty")
    if mode not in _MODE_WORKLOAD:
        raise ValueError(f"unknown mode: {mode}")
    qwen_client.check_health(base_url)
    max_tokens = (
        facets.UNIVERSAL_MAX_OUTPUT_TOKENS if mode == "visual" else 1024
    )
    job_id = qwen_client.submit_job(
        base_url,
        [
            {
                "custom_key": r["custom_key"],
                "prompt": r["prompt"],
                "images": r.get("images") or [],
            }
            for r in items
        ],
        model=model,
        max_tokens=max_tokens,
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
    """Retrieve terminal facet jobs; land VERBATIM, then parse and write gold.

    ADR-0011/0013 sequencing per retrieved item: the provider response is
    landed VERBATIM into ``bronze_enrichment_raw`` — under this pass's
    workload (visual ≠ text), with ``run_id`` = the service job id, which
    makes re-harvesting the same job idempotent on the natural key — BEFORE
    any parsing. Only then is the response parsed, validated and merged into
    ``gold_growth_facets``. Failures land with ``ok=False`` and a populated
    ``error_message``; failure is READ from bronze, never inferred from a
    missing conformed row.

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
                provider=res.provider or "qwen",
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
                parsed = facets.parse_text_response(text)
                if parsed["errors"] or parsed["text_facets"] is None:
                    logger.warning(
                        "Post %s text facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["text_facets"],
                    model=res_model,
                    prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
                )
            else:
                n_media = _n_media_for(conn, post_id)
                parsed = facets.parse_universal_response(text, n_media)
                if parsed["errors"] or parsed["visual_facets"] is None:
                    logger.warning(
                        "Post %s visual facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["visual_facets"],
                    model=res_model,
                    content_summary=parsed["content_summary"],
                    image_summaries=parsed["image_summaries"],
                )
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


def _n_media_for(conn, post_id: str) -> int:
    row = conn.execute(
        "SELECT media_files FROM silver_ig_posts WHERE post_id = ?", [post_id]
    ).fetchone()
    if not row or not row[0]:
        return 0
    try:
        urls = json.loads(row[0])
    except json.JSONDecodeError:
        return 0
    return len(urls) if isinstance(urls, list) else 0
