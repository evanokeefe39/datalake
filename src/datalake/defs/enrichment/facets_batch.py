"""Batch-native growth-facets enrichment — facets → ``gold_growth_facets``.

Mirrors the proven IG-gold batch-native flow (ADR-0007/0008) but targets the
growth-facets table. The IG-gold flow, as implemented:

    ig_posts_gen_batches (ops.sqlite batch_jobs/batch_items queue)
      → media_upload_pending_batches_op (media_cache.lookup_or_upload_all
        pre-uploads File API URIs for pending candidates)
      → submit_gemini_batches_job
          (analysis.build_requests_for_items: per item reads silver
          caption + media_files, resolves media with tier + per-item token
          gates via analysis._resolve_media_for_post, builds
          ``{"custom_key": batch_items.id, "prompt", "post_id", "domain",
          "media_files": [...]}`` — the generic ``gemini_batch.submit`` then
          chunks by estimated in-flight tokens and posts InlinedRequests,
          media Parts included, to the Gemini BATCH API)
      → gemini_batch_harvest_sensor (cursor over batch_jobs.gemini_batch_name
        chunks) → gemini_batch_harvest job
          (gemini_batch.poll/job_state/is_terminal per chunk; on SUCCEEDED
          gemini_batch.retrieve → {custom_key: {ok, text, error}};
          harvest.apply_retrieved → analysis.write_gold upsert + queue
          bookkeeping)

This module carries the SAME verbs but without the ops.sqlite work queue —
a facet batch is a one-shot driver submission (custom_key IS the post_id, so
no batch_items indirection is needed), with resume state persisted in a small
``facets_batch_jobs`` ledger table on ops.sqlite (per-chunk status +
``meta_json`` carrying the per-request media counts so harvest validates
carousel ``image_summaries`` against the media actually SENT, not the silver
row):

    build_facets_batch_requests  (mirror of build_requests_for_items)
      → gemini_batch.submit      (identical chunk caps / media Parts path)
      → wait_for_facets_batches  (poll to terminal state, bounded)
      → harvest_facets_batches   (retrieve → parse_universal_response /
        parse_text_response → validate → facets.write_gold_facets_pass_conn
        MERGE upsert into gold_growth_facets)

Two passes, partial-storage contract (US-EFAC-3/4):

- **visual** — ``build_growth_facets_prompt`` + media Parts (File API URIs or
  inline image bytes via ``lookup_or_upload_all``); parse with
  ``parse_universal_response``; writes visual sub-fields + content_summary +
  image_summaries.
- **text** — ``build_text_facets_prompt`` + NO media (caption only); parse
  with ``parse_text_response``; merges text sub-fields.

Each write merges into the row's existing ``growth_facets_json`` so the stored
row is the union of both passes and satisfies the full V3 schema.

ADR-0008 seam: this module drives the Gemini API (like submit/harvest) but
defines no Dagster ops; it is consumed by ``scripts/enrich_facets_batch.py``.
"""

from __future__ import annotations

import json
import logging
import time

from datalake.defs.common.resources import GeminiResource, SQLiteResource
from datalake.defs.enrichment import analysis as ew
from datalake.defs.enrichment import facets, gemini_batch
from datalake.defs.enrichment.prompts import (
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    _DEFAULT_GEMINI_MODEL,
    build_growth_facets_prompt,
    build_text_facets_prompt,
)

logger = logging.getLogger("enrichment.facets_batch")

# Batch list-price discount (Gemini Batch API is 50% of interactive pricing).
BATCH_DISCOUNT = 0.5

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = facets.UNIVERSAL_MAX_OUTPUT_TOKENS
_EST_OUTPUT_TOKENS_TEXT = 1024

_TEXT_REQUIRED = (
    "hook_content", "hook_type", "is_sponsored", "sponsorship_signal",
    "claimed_results", "cta_type", "audience_named", "value_depth",
    "replicable_tactic", "evidence", "brand_safety",
)

# ── Resume ledger (ops.sqlite) ───────────────────────────────────────────────

_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS facets_batch_jobs (
    id INTEGER PRIMARY KEY,
    mode VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    gemini_batch_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    n_requests INTEGER NOT NULL,
    est_tokens INTEGER NOT NULL,
    meta_json VARCHAR NOT NULL DEFAULT '{}',
    created_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
)
"""


def _ensure_ledger(ops: SQLiteResource) -> None:
    conn = ops.get_connection()
    try:
        conn.execute(_LEDGER_DDL)
        conn.commit()
    finally:
        conn.close()


def _record_jobs(
    ops: SQLiteResource,
    mode: str,
    model: str,
    names: list[str],
    n_requests: int,
    est_tokens: int,
    meta: dict,
) -> None:
    now = ew._now_iso()
    conn = ops.get_connection()
    try:
        for name in names:
            conn.execute(
                "INSERT INTO facets_batch_jobs "
                "(mode, model, gemini_batch_name, status, n_requests, "
                " est_tokens, meta_json, created_at, updated_at) "
                "VALUES (?, ?, ?, 'SUBMITTED', ?, ?, ?, ?, ?)",
                [mode, model, name, n_requests, est_tokens,
                 json.dumps(meta), now, now],
            )
        conn.commit()
    finally:
        conn.close()


def _set_ledger_status(ops: SQLiteResource, name: str, status: str) -> None:
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE facets_batch_jobs SET status = ?, updated_at = ? "
            "WHERE gemini_batch_name = ?",
            [status, ew._now_iso(), name],
        )
        conn.commit()
    finally:
        conn.close()


def pending_ledger_jobs(ops: SQLiteResource) -> list[dict]:
    """Submitted-but-not-yet-harvested facet batch jobs (resume-safe)."""
    _ensure_ledger(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT gemini_batch_name, mode, model, status, meta_json FROM "
            "facets_batch_jobs WHERE status = 'SUBMITTED' ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {"name": r[0], "mode": r[1], "model": r[2], "status": r[3],
         "meta": json.loads(r[4]) if r[4] else {}}
        for r in rows
    ]


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
    if mode == "visual":
        rows = conn.execute(
            "SELECT post_id FROM gold_growth_facets WHERE prompt_hash = ?",
            [CURRENT_FACETS_PROMPT_HASH],
        ).fetchall()
        return {r[0] for r in rows}
    # text: done when the stored JSON carries every required text field.
    rows = conn.execute(
        "SELECT post_id, growth_facets_json FROM gold_growth_facets "
        "WHERE prompt_hash = ?",
        [CURRENT_TEXT_FACETS_PROMPT_HASH],
    ).fetchall()
    done = set()
    for post_id, blob in rows:
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
    gemini: GeminiResource,
    conn,
    targets: list[dict],
    mode: str,
    model: str = _DEFAULT_GEMINI_MODEL,
) -> list[dict]:
    """Build batch API requests for facet targets.

    Mirror of ``analysis.build_requests_for_items``: visual targets resolve
    media via the shared ``_resolve_media_for_post`` (byte cache → File API /
    inline images, tier + per-item token gates) and send
    ``build_growth_facets_prompt(caption, n_media)`` with media Parts; text
    targets send ``build_text_facets_prompt(caption)`` with NO media.
    ``custom_key`` is the post_id so harvest maps responses directly.
    Targets whose media resolution fails are logged and dropped (the driver
    re-discovers them on the next run).
    """
    requests: list[dict] = []
    for t in targets:
        post_id = t["post_id"]
        caption = t["caption"]
        if mode == "visual":
            try:
                media_files = ew._resolve_media_for_post(
                    ops, gemini, post_id, t["media_files"], inline_images=True
                )
            except Exception as exc:
                logger.warning(
                    "Media resolution failed on %s: %s", post_id, str(exc)[:120]
                )
                continue
            if not media_files:
                # Terminal per-item condition (gated out / unresolvable) —
                # same semantics as the interactive universal call.
                logger.info("Post %s: no usable media — skipped", post_id)
                continue
            req = {
                "custom_key": post_id,
                "prompt": build_growth_facets_prompt(caption, len(media_files)),
                "post_id": post_id,
                "media_files": media_files,
            }
        else:
            req = {
                "custom_key": post_id,
                "prompt": build_text_facets_prompt(caption),
                "post_id": post_id,
            }
        requests.append(req)
    return requests


# ── Cost projection ──────────────────────────────────────────────────────────


def estimate_facets_cost(requests: list[dict]) -> tuple[int, float]:
    """(estimated input tokens, batch-projected USD) for a request list.

    Batch list price is 50% of interactive (``BATCH_DISCOUNT``); prices are
    the same env-overridable Tier-1 flash-lite list estimates as facets.py.
    Media input tokens are included via ``request_estimate_tokens``.
    """
    input_tokens = sum(gemini_batch.request_estimate_tokens(r) for r in requests)
    out_tokens = sum(
        _EST_OUTPUT_TOKENS_VISUAL if r.get("media_files") else _EST_OUTPUT_TOKENS_TEXT
        for r in requests
    )
    cost = (
        input_tokens / 1_000_000 * facets._INPUT_PRICE_PER_M * BATCH_DISCOUNT
        + out_tokens / 1_000_000 * facets._OUTPUT_PRICE_PER_M * BATCH_DISCOUNT
    )
    return input_tokens, cost


# ── Submit / wait / harvest ──────────────────────────────────────────────────


def submit_facets_batch(
    ops: SQLiteResource,
    gemini: GeminiResource,
    requests: list[dict],
    mode: str,
    model: str = _DEFAULT_GEMINI_MODEL,
    display_name: str = "facets-batch",
) -> list[str]:
    """Submit facet requests via the shared ``gemini_batch.submit`` (which
    enforces the tier gate + in-flight token caps) and persist the chunk
    names in the resume ledger. Returns the Gemini batch job names."""
    if not requests:
        raise ValueError("requests must not be empty")
    _ensure_ledger(ops)
    input_tokens, _ = estimate_facets_cost(requests)
    meta = {
        "n_media": {
            r["custom_key"]: len(r.get("media_files") or []) for r in requests
        }
    }
    names = gemini_batch.submit(gemini, model, requests, display_name=display_name)
    _record_jobs(ops, mode, model, names, len(requests), input_tokens, meta)
    logger.info(
        "Facets batch (%s): %d requests → %d Gemini job(s)",
        mode, len(requests), len(names),
    )
    return names


def wait_for_facets_batches(
    gemini: GeminiResource,
    names: list[str],
    poll_seconds: int = 60,
    timeout_seconds: int = 24 * 3600,
) -> dict[str, str]:
    """Poll job names until each reaches a terminal state (bounded).

    Returns ``{name: terminal_state}``. Raises ``RuntimeError`` on timeout.
    """
    remaining = dict.fromkeys(names)
    deadline = time.monotonic() + timeout_seconds
    while remaining:
        for name in list(remaining):
            state = gemini_batch.job_state(gemini_batch.poll(gemini, name))
            if gemini_batch.is_terminal(state):
                remaining[name] = state
                del remaining[name]
        if remaining and time.monotonic() > deadline:
            raise RuntimeError(
                f"Timed out waiting for facet batches: {list(remaining)}"
            )
        if remaining:
            time.sleep(poll_seconds)
    return remaining


def harvest_facets_batches(
    ops: SQLiteResource,
    gemini: GeminiResource,
    conn,
    names: list[str] | None = None,
    model: str = _DEFAULT_GEMINI_MODEL,
) -> dict:
    """Retrieve terminal facet batches, parse, validate, write gold rows.

    For each SUBMITTED ledger job (or the explicit ``names``): poll; on
    SUCCEEDED, ``gemini_batch.retrieve`` maps responses by ``custom_key``
    (the post_id); visual responses go through ``parse_universal_response`` →
    ``validate_visual_facets`` (inside the parser), text responses through
    ``parse_text_response`` → ``validate_text_facets``; validated sub-fields
    merge into ``gold_growth_facets`` via ``write_gold_facets_pass_conn``.
    Failed jobs flip the ledger row to JOB_FAILED (their targets are NOT
    recorded as done — the driver re-discovers them).
    """
    _ensure_ledger(ops)
    pending = (
        [{"name": n, "mode": None, "meta": {}} for n in names]
        if names
        else pending_ledger_jobs(ops)
    )
    written = invalid = 0
    for job in pending:
        name = job["name"]
        mode = job.get("mode")
        meta = job.get("meta") or {}
        state = gemini_batch.job_state(gemini_batch.poll(gemini, name))
        if not gemini_batch.is_terminal(state):
            continue
        if state != "SUCCEEDED":
            logger.error("Facets batch job %s %s", name, state)
            _set_ledger_status(ops, name, "JOB_FAILED")
            continue
        results = gemini_batch.retrieve(gemini, name)
        for post_id, res in results.items():
            if not res.get("ok"):
                logger.warning(
                    "Post %s batch error: %s", post_id, res.get("error")
                )
                continue
            if mode == "text":
                parsed = facets.parse_text_response(res["text"])
                if parsed["errors"] or parsed["text_facets"] is None:
                    logger.warning(
                        "Post %s text facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["text_facets"],
                    model=model, prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
                )
            else:
                sent = (meta.get("n_media") or {}).get(post_id)
                n_media = int(sent) if sent is not None else _n_media_for(
                    conn, post_id
                )
                parsed = facets.parse_universal_response(res["text"], n_media)
                if parsed["errors"] or parsed["visual_facets"] is None:
                    logger.warning(
                        "Post %s visual facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["visual_facets"],
                    model=model,
                    content_summary=parsed["content_summary"],
                    image_summaries=parsed["image_summaries"],
                )
            written += 1
        _set_ledger_status(ops, name, "RETRIEVED")
    return {"written": written, "invalid": invalid, "jobs": len(pending)}


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
