"""Enrichment analysis core — canonical implementation (ADR-0007/0008).

Batch-native migration, Phase 5: the enrichment logic lives HERE as the
single source of truth, consumed by the Dagster-native submit job
(``defs.enrichment.submit``), the harvest job (``defs.enrichment.harvest``),
and the out-of-band interactive tool (``scripts/enrich_interactive.py``):

- request building for the Gemini BATCH API (``build_requests_for_items``),
- media resolution with tier + token gates (``_resolve_media_for_post``),
- the interactive per-post analysis (``process_item``),
- the consolidated gold upsert (``write_gold`` — one writer shared by the
  submit/harvest jobs and the interactive tool),
- the 429/File-API error taxonomy + backoff helpers,
- the domain dispatch tables (``_SILVER_TABLES`` / ``_PROMPTS``).

ADR-0008 seam: this module TOUCHES the Gemini API (media upload, analysis
calls) — it is an enrichment seam module, NOT a hermetic one. The seam guard
(``media_upload.seam_violations``) only scans the ``_PURE_MODULES`` for API
markers and requires seam tags on ops; this module defines no ops.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    _now_iso,
    claim_pending_items,
    complete_item,
    fail_item,
)
from datalake.defs.enrichment.media_cache import lookup_or_upload_all
from datalake.defs.enrichment.prompts import (
    _DEFAULT_GEMINI_MODEL,
    CURRENT_PROMPT_HASH,
    IG_GOLD_PROMPT,
)
from datalake.defs.instagram.config import GeminiTierConfig

logger = logging.getLogger("enrichment.analysis")

# ── Domain dispatch tables ───────────────────────────────────────────────────

_SILVER_TABLES: dict[str, str] = {
    "instagram": "silver_ig_posts",
}

_PROMPTS: dict[str, str] = {
    "instagram": IG_GOLD_PROMPT,
}

# ── Token budget constants ───────────────────────────────────────────────────

_TOKENS_PER_SECOND_VIDEO_LOW = 98  # low resolution: 66 video + 32 audio tokens/sec
_PER_ITEM_TOKEN_CAP = 250_000  # conservative per-item cap for video processing

# ── Rate-limit helpers ───────────────────────────────────────────────────────

_QUOTA_KEYWORDS = {
    "quota", "insufficient", "daily limit", "insufficient_quota",
}


def _is_quota_exhausted(exc: Exception, error_text: str) -> bool:
    """Return True if the exception indicates daily quota (RPD) exhaustion.

    The structured ``insufficient_quota`` marker (from the API's error.details)
    is authoritative; fall back to quota-specific keywords. A bare 429 or
    "rate limit" must NOT match here — that is a burst, handled by
    ``_is_rate_limited``.
    """
    lower = error_text.lower()
    details = str(getattr(exc, "details", "")).lower()
    if "insufficient_quota" in details or "insufficient_quota" in lower:
        return True
    return any(kw in lower for kw in _QUOTA_KEYWORDS)


def _is_rate_limited(exc: Exception, error_text: str) -> bool:
    """Return True if the exception is a rate-limit burst (RPM/TPM)."""
    lower = error_text.lower()
    details = str(getattr(exc, "details", "")).lower()
    if "rate_limit_exceeded" in details or "rate_limit_exceeded" in lower:
        return True
    return "429" in lower or "rate limit" in lower


def _quota_reset_backoff() -> int:
    """Estimate seconds until next UTC midnight for quota reset."""
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int((tomorrow - now).total_seconds()) + 60


def _exponential_backoff(attempt: int) -> float:
    """Exponential backoff with jitter: 2^attempt + random(0,1) seconds."""
    return 2**attempt + random.uniform(0, 1)


def _item_attempts(ops: SQLiteResource, item_id: int) -> int:
    """Current attempt count for an item (0 if absent)."""
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT attempts FROM batch_items WHERE id = ?", [item_id]
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


# ── File API error classification ────────────────────────────────────────────

_FILE_API_KEYWORDS = {
    "file api", "files/", "upload", "file state", "timeouterror",
    "urllib", "urlretrieve", "download",
}


def _is_file_api_error(exc: Exception, error_text: str) -> bool:
    """Return True if the exception is from media download or File API upload.

    File API errors are per-item — they should NOT trigger batch-wide
    quota rescheduling like generation 429s do.
    """
    lower = error_text.lower()
    return any(kw in lower for kw in _FILE_API_KEYWORDS)


# ── Dead letter ──────────────────────────────────────────────────────────────


def _dead_letter_insert(
    ops: SQLiteResource, post_id: str, domain: str, error: str, attempts: int
) -> None:
    """Insert a failed item into dead_letter table."""
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("dead_letter"))
        conn.execute(
            "INSERT OR REPLACE INTO dead_letter (post_id, domain, error, attempts, failed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [post_id, domain, error, attempts, _now_iso()],
        )
        conn.commit()
    finally:
        conn.close()


# ── Gold upsert (single source of truth — worker + harvest share this) ───────


def write_gold(
    duckdb: DuckDBResource,
    post_id: str,
    domain: str,
    result: str,
    model: str = _DEFAULT_GEMINI_MODEL,
) -> None:
    """Upsert a validated analysis into gold_analyses (ordering guard).

    Idempotent by PK ``(post_id, domain)``; the ``WHERE analysed_at`` guard
    means a stale concurrent write can never clobber a newer analysis.
    """
    now = _now_iso()
    with duckdb.get_connection() as conn:
        conn.execute(
            """INSERT INTO gold_analyses
               (post_id, domain, prompt_hash, model, result_json, analysed_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT (post_id, domain) DO UPDATE SET
                   prompt_hash = excluded.prompt_hash,
                   model = excluded.model,
                   result_json = excluded.result_json,
                   analysed_at = excluded.analysed_at
               WHERE gold_analyses.analysed_at IS NULL
                  OR excluded.analysed_at > gold_analyses.analysed_at""",
            [post_id, domain, CURRENT_PROMPT_HASH, model, result, now],
        )


# Legacy alias retained for existing importers (enrich_interactive, tests).
_write_gold = write_gold


# ── Media resolution ─────────────────────────────────────────────────────────


def _resolve_media_for_post(
    ops: SQLiteResource,
    gemini: GeminiResource,
    post_id: str,
    media_files_json: str | None,
    inline_images: bool = False,
) -> list:
    """Resolve a post's media to File API URIs, applying the tier + token gates.

    Shared by the interactive (``process_item``) and batch
    (``build_requests_for_items``) paths so multimodal handling stays consistent.
    ``inline_images`` (batch path only) serves small images as inline bytes —
    no File API upload round-trip. Interactive stays on the File API.
    Returns the MediaFile dicts to send, or [] for a text-only fallback (no
    media, FREE-tier video gate, or per-item video token cap exceeded).
    """
    tier_cfg = GeminiTierConfig.detect()
    media_files = lookup_or_upload_all(
        ops, gemini, media_files_json, inline_images=inline_images
    )

    # Tier gate: FREE tier skips video — text-only fallback
    if media_files and not tier_cfg.supports_video:
        logger.info(
            "Post %s has %d media files but tier is %s — text-only fallback",
            post_id, len(media_files), tier_cfg.tier.value,
        )
        return []

    # Token budget check: drop video if estimated tokens exceed the per-item cap
    if media_files:
        total_estimated = 0
        for mf in media_files:
            if mf.get("mime_type", "").startswith("video/"):
                duration = mf.get("duration_seconds") or 0
                if duration > 0:
                    total_estimated += duration * _TOKENS_PER_SECOND_VIDEO_LOW
                else:
                    total_estimated += 60 * _TOKENS_PER_SECOND_VIDEO_LOW  # assume 1 min
        if total_estimated > _PER_ITEM_TOKEN_CAP:
            logger.warning(
                "Post %s video token estimate %d > cap %d — text-only fallback",
                post_id, total_estimated, _PER_ITEM_TOKEN_CAP,
            )
            return []
    return media_files


# ── Interactive item processing ──────────────────────────────────────────────


def process_item(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    item: dict,
) -> bool:
    """Process a single batch item: read silver → Gemini → write gold.

    Returns True on success, False on failure.
    Edge cases handled:
    - Unknown domain → skips with completion (clears from pipeline)
    - Post not found in silver → skips with completion
    - Empty caption → skips with completion (no Gemini call)
    - Rate limit → backoff + reschedule
    - Quota exhausted → raises to caller for global reschedule
    - Max attempts → dead letter
    """
    payload = json.loads(item["payload"])
    post_id = payload["post_id"]
    domain = payload["domain"]
    item_id = item["id"]

    table = _SILVER_TABLES.get(domain)
    if not table:
        complete_item(ops, item_id)
        logger.info("Unknown domain %s for post %s — completed", domain, post_id)
        return True

    # Read caption + media from silver
    with duckdb.get_connection() as conn:
        row = conn.execute(
            f"SELECT caption, media_files FROM {table} WHERE post_id = ?",
            [post_id],
        ).fetchone()

    if not row:
        complete_item(ops, item_id)
        logger.info("Post %s not in silver — completed", post_id)
        return True

    caption = row[0] or ""
    if not caption.strip():
        complete_item(ops, item_id)
        logger.info("Post %s has empty caption — completed", post_id)
        return True

    # Media: download + upload to Gemini File API (or cache hit), tier + token gated
    media_files = _resolve_media_for_post(ops, gemini, post_id, row[1])

    # Analyze via Gemini
    prompt_text = _PROMPTS.get(domain, IG_GOLD_PROMPT) + "\n" + caption
    analyze_kwargs: dict = {}
    if media_files:
        analyze_kwargs["media_files"] = media_files
    result = gemini.analyze(prompt_text, **analyze_kwargs)
    # Validate JSON
    try:
        json.loads(result)
    except json.JSONDecodeError:
        raise ValueError(f"Gemini returned invalid JSON for post {post_id}")

    # Write gold_analyses with ordering guard
    write_gold(duckdb, post_id, domain, result)

    complete_item(ops, item_id)
    return True


# ── Gemini BATCH API request building ────────────────────────────────────────


def build_requests_for_items(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    items: list[dict],
    model: str = _DEFAULT_GEMINI_MODEL,
) -> list[dict]:
    """Build multimodal batch API requests for claimed items.

    Returns ``{"custom_key", "prompt", "post_id", "domain", "media_files": [...]}``
    dicts — ``media_files`` present (File API URIs) when the post has media that
    passes the tier + per-item token gates. Items without silver rows or with
    empty captions complete immediately (no API call) — mirrors interactive
    edge-case handling. A per-item media resolution failure (cache-miss CDN
    download 403, File API upload error) fails that item with backoff instead
    of aborting the whole submit. MIXED MEDIA POLICY: if a post has some
    cached and some dead URLs, the whole post is failed (retry/dead-letter) —
    ``lookup_or_upload_all`` raises on the first unresolvable URL, and
    submitting partial media would silently change the analysis input, which
    neither interactive nor batch tolerates.
    """
    requests: list[dict] = []
    for item in items:
        payload = json.loads(item["payload"])
        post_id = payload["post_id"]
        domain = payload["domain"]
        prompt_template = _PROMPTS.get(domain, IG_GOLD_PROMPT)
        table = _SILVER_TABLES.get(domain)
        if not table:
            complete_item(ops, item["id"])
            continue
        with duckdb.get_connection() as conn:
            row = conn.execute(
                f"SELECT caption, media_files FROM {table} WHERE post_id = ?",
                [post_id],
            ).fetchone()
        caption = (row[0] if row else "") or ""
        if not caption.strip():
            complete_item(ops, item["id"])
            logger.info("Post %s has empty caption — completed", post_id)
            continue
        media_files = None
        if row and row[1]:
            try:
                media_files = _resolve_media_for_post(
                    ops, gemini, post_id, row[1], inline_images=True
                )
            except Exception as exc:
                # Media resolution (CDN download on a genuine cache miss, File
                # API upload) is strictly per-item work — generation quota
                # errors can never originate here, so ANY exception is a
                # media failure for this post only. Mirrors interactive
                # process_item, which routes all per-item exceptions to
                # fail_item/dead_letter instead of aborting the run.
                error_text = str(exc)
                attempts = fail_item(
                    ops, item["id"], error_text,
                    backoff=_exponential_backoff(_item_attempts(ops, item["id"])),
                )
                logger.warning(
                    "Media resolution failed on %s (attempt %d): %s",
                    post_id, attempts, error_text[:120],
                )
                if attempts >= MAX_ATTEMPTS:
                    _dead_letter_insert(
                        ops, post_id, domain, error_text, attempts,
                    )
                    logger.error(
                        "Post %s moved to dead_letter after %d media attempts",
                        post_id, attempts,
                    )
                continue  # drop this item from the submit, keep the rest
        req: dict = {
            "custom_key": str(item["id"]),
            "prompt": f"{prompt_template}\n{caption}",
            "post_id": post_id,
            "domain": domain,
        }
        if media_files:
            req["media_files"] = media_files
        requests.append(req)
    return requests
