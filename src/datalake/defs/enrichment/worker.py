"""Enrichment worker — processes claimed queue items via Gemini.

The worker reads post data from silver (not the queue), calls Gemini,
writes gold_analyses, and handles rate limiting and dead_letter routing.
"""

from __future__ import annotations
import json
import random
from dagster import (
    AssetKey,
    AssetMaterialization,
    job,
    op,
)

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.enrichment.assets import ensure_gold_analyses
from datalake.defs.enrichment.media_cache import lookup_or_upload
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH, IG_GOLD_PROMPT
from datalake.defs.enrichment.queue import MAX_ATTEMPTS, complete, delete, fail, reschedule

# ── Domain dispatch tables ──────────────────────────────────────────────────

_SILVER_TABLES: dict[str, str] = {
    "instagram": "silver_ig_posts",
    "tiktok": "silver_tiktok_posts",
}

_PROMPTS: dict[str, str] = {
    "instagram": IG_GOLD_PROMPT,
    # tiktok prompt TBD when TikTok scraper lands
}

# ── Rate-limit helpers ──────────────────────────────────────────────────────


def _is_quota_exhausted(exc: Exception, error_text: str) -> bool:
    """Return True if the exception indicates daily quota (RPD) exhaustion."""
    lower = error_text.lower()
    quota_keywords = [
        "insufficient_quota",
        "resource has been exhausted",
        "429 resource_exhausted",
        "quota exceeded",
        "resource_exhausted",
    ]
    return any(kw in lower for kw in quota_keywords)


def _is_rate_limited(exc: Exception, error_text: str) -> bool:
    """Return True if the exception is a rate-limit burst (RPM/TPM)."""
    lower = error_text.lower()
    rate_keywords = ["rate_limit_exceeded", "rate limit", "too many requests"]
    if any(kw in lower for kw in rate_keywords):
        return True
    # Distinguish from quota exhaustion
    if "429" in lower and not _is_quota_exhausted(exc, error_text):
        return True
    return False


def _quota_reset_backoff() -> int:
    """Estimate seconds until next UTC midnight for quota reset."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta

    tomorrow = tomorrow + timedelta(days=1)
    return int((tomorrow - now).total_seconds()) + 60  # +60s safety margin


def _exponential_backoff(attempt: int) -> int:
    """Exponential backoff with jitter: 2^attempt + random(0,1) seconds."""
    return int(2**attempt + random.uniform(0, 1))


def _dead_letter_insert(
    ops: SQLiteResource, post_id: str, domain: str, error: str, attempts: int
) -> None:
    """Insert a failed item into dead_letter."""
    from datalake.defs.enrichment.queue import _now_iso

    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO dead_letter (post_id, domain, error, attempts, failed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [post_id, domain, error, attempts, _now_iso()],
        )
        conn.commit()
    finally:
        conn.close()


@op
def enrichment_worker(context) -> None:
    """Process claimed queue items: read silver → Gemini → write gold.

    Receives ``post_ids`` and ``domains`` via run_config from the sensor.
    Handles rate limiting per-item, quota exhaustion globally,
    and routes exhausted items to dead_letter.
    """
    post_ids: list[str] = context.run_config["post_ids"]  # type: ignore[index]
    domains: list[str] = context.run_config["domains"]  # type: ignore[index]

    ops: SQLiteResource = context.resources.ops
    duck: DuckDBResource = context.resources.duckdb
    gemini: GeminiResource = context.resources.gemini

    ensure_gold_analyses(duck)

    processed = 0
    for i, (post_id, domain) in enumerate(zip(post_ids, domains)):
        try:
            table = _SILVER_TABLES.get(domain)
            if not table:
                # Unknown domain — complete to clear from queue
                complete(ops, post_id, domain)
                continue

            # Read caption + media from silver (latest, not from queue)
            with duck.get_connection() as conn:
                row = conn.execute(
                    f"SELECT caption, media_files FROM {table} WHERE post_id = ?",
                    [post_id],
                ).fetchone()

            if not row:
                # Post not in silver — complete to clear from queue
                complete(ops, post_id, domain)
                continue

            caption = row[0] or ""
            if not caption.strip():
                # Empty caption — complete, no Gemini call needed
                complete(ops, post_id, domain)
                continue
            # Media cache: URL hash → File API URI (pre-upload for future use)
            lookup_or_upload(ops, gemini, row[1])
            # Analyze via Gemini
            prompt_text = _PROMPTS.get(domain, IG_GOLD_PROMPT) + "\n" + caption
            result = gemini.analyze(prompt_text)

            # Validate JSON
            json.loads(result)

            # Write gold_analyses with ordering guard
            from datalake.defs.enrichment.queue import _now_iso

            now = _now_iso()
            with duck.get_connection() as conn:
                conn.execute(
                    """INSERT INTO gold_analyses
                       (post_id, domain, prompt_hash, result_json, analysed_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (post_id, domain) DO UPDATE SET
                           prompt_hash = excluded.prompt_hash,
                           result_json = excluded.result_json,
                           analysed_at = excluded.analysed_at
                       WHERE gold_analyses.analysed_at IS NULL
                          OR excluded.analysed_at > gold_analyses.analysed_at""",
                    [post_id, domain, CURRENT_PROMPT_HASH, result, now],
                )

            complete(ops, post_id, domain)
            processed += 1

        except Exception as exc:
            error_text = str(exc)

            if _is_quota_exhausted(exc, error_text):
                # Global condition — reschedule ALL remaining without burning attempts
                for rid, rdom in zip(post_ids[i:], domains[i:]):
                    reschedule(ops, rid, rdom, error_text, _quota_reset_backoff())
                break

            attempts = 0
            if _is_rate_limited(exc, error_text):
                attempts = fail(
                    ops, post_id, domain, error_text,
                    backoff=_exponential_backoff(1),
                )
            else:
                attempts = fail(ops, post_id, domain, error_text, backoff=0)

            # Dead letter check
            if attempts >= MAX_ATTEMPTS:
                _dead_letter_insert(ops, post_id, domain, error_text, attempts)
                delete(ops, post_id, domain)

    # Emit partial materialization event
    from datalake.defs.enrichment.queue import depth

    remaining = depth(ops)
    yield AssetMaterialization(
        asset_key=AssetKey("gold_analyses"),
        metadata={
            "items_processed": processed,
            "queue_depth_remaining": remaining,
        },
    )


# ── Job ─────────────────────────────────────────────────────────────────────


@job(
    name="enrichment_job",
    description="Process claimed enrichment queue items via Gemini.",
)
def enrichment_job() -> None:
    """Job wrapper around enrichment_worker — launched by the sensor."""
    enrichment_worker()
