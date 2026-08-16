"""Standalone enrichment worker — processes batch items via Gemini.

Runs outside Dagster. Reads pending batches from ops.sqlite, calls Gemini
for each item, writes gold_analyses to DuckDB, and POSTs materialization
events to Dagster when batches complete.

Usage::

    uv run python scripts/enrichment_worker.py                # Process next pending batch
    uv run python scripts/enrichment_worker.py --dry-run      # Show state, don't process
    uv run python scripts/enrichment_worker.py --batch-id 3   # Process specific batch
    uv run python scripts/enrichment_worker.py --limit 10     # Process at most 10 items
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import urllib.request
from datetime import datetime, timezone

from dotenv import load_dotenv

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl
from datalake.defs.enrichment.assets import ensure_gold_analyses
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    _now_iso,
    batch_progress,
    claim_batch,
    claim_pending_items,
    complete_item,
    fail_item,
    mark_complete,
)
from datalake.defs.enrichment.media_cache import lookup_or_upload_all
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH, IG_GOLD_PROMPT
from datalake.defs.instagram.config import GeminiTierConfig

load_dotenv()

logger = logging.getLogger("enrichment_worker")

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

# ── Dagster API ──────────────────────────────────────────────────────────────

_DAGSTER_URL = "http://localhost:3000"


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
    tomorrow = (now + __import__("datetime").timedelta(days=1)).replace(
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


# ── Item processing ──────────────────────────────────────────────────────────


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

    # Media: download + upload to Gemini File API (or cache hit)
    tier_cfg = GeminiTierConfig.detect()
    media_files = lookup_or_upload_all(ops, gemini, row[1])

    # Tier gate: FREE tier skips video — text-only fallback
    if media_files and not tier_cfg.supports_video:
        logger.info(
            "Post %s has %d media files but tier is %s — text-only fallback",
            post_id, len(media_files), tier_cfg.tier.value,
        )
        media_files = []

    # Token budget check: skip video if estimated tokens exceed per-item cap
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
            media_files = []

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
    now = _now_iso()
    with duckdb.get_connection() as conn:
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

    complete_item(ops, item_id)
    return True


# ── Batch processing ─────────────────────────────────────────────────────────


def process_batch(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    batch: dict,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Process items in a batch. Returns summary dict.

    Handles quota exhaustion by rescheduling remaining items, rate limits
    with backoff, and max-attempt dead letter routing.
    """
    job_id = batch["id"]
    all_payloads = batch["payloads"]

    if dry_run:
        progress = batch_progress(ops, job_id)
        logger.info(
            "DRY RUN — batch %d: %d total, %d processed, %d failed, %d pending",
            job_id,
            progress["total"],
            progress["processed"],
            progress["failed"],
            progress["pending"],
        )
        return {"processed": 0, "failed": 0, "dry_run": True}

    ensure_gold_analyses(duckdb)

    total_processed = 0
    total_failed = 0
    items_to_process = limit if limit else len(all_payloads)
    processed_count = 0

    while processed_count < items_to_process:
        # Claim a chunk of pending items
        chunk = claim_pending_items(ops, job_id, limit=5)
        if not chunk:
            break

        for item in chunk:
            if processed_count >= items_to_process:
                break

            # Extract identity from payload for logging/error routing
            item_payload = json.loads(item["payload"])
            item["_post_id"] = item_payload["post_id"]
            item["_domain"] = item_payload["domain"]

            try:
                success = process_item(ops, duckdb, gemini, item)
                if success:
                    total_processed += 1
                else:
                    total_failed += 1
                processed_count += 1
            except Exception as exc:
                error_text = str(exc)

                # File API errors are per-item — never abort the batch
                if _is_file_api_error(exc, error_text):
                    attempts = fail_item(
                        ops, item["id"], error_text,
                        backoff=_exponential_backoff(_item_attempts(ops, item["id"])),
                    )
                    logger.warning(
                        "File API error on %s (attempt %d): %s",
                        item["_post_id"], attempts, error_text[:120],
                    )
                    if attempts >= MAX_ATTEMPTS:
                        _dead_letter_insert(
                            ops, item["_post_id"], item["_domain"], error_text, attempts,
                        )
                        logger.error(
                            "Post %s moved to dead_letter after %d File API attempts",
                            item["_post_id"], attempts,
                        )
                    total_failed += 1
                    processed_count += 1
                    continue

                if _is_quota_exhausted(exc, error_text):
                    # Global condition — reschedule the current item and all
                    # remaining claimed items without burning attempts (quota
                    # is not their fault). Halts the batch for today.
                    backoff_secs = _quota_reset_backoff()
                    logger.warning(
                        "Quota exhausted — rescheduling remaining items. "
                        "Backoff: %ds. Processed: %d/%d",
                        backoff_secs,
                        total_processed,
                        items_to_process,
                    )
                    for remaining in chunk[chunk.index(item):]:
                        fail_item(
                            ops, remaining["id"], error_text,
                            backoff=backoff_secs, preserve_attempts=True,
                        )
                    return {
                        "processed": total_processed,
                        "failed": total_failed,
                        "quota_exhausted": True,
                    }

                if _is_rate_limited(exc, error_text):
                    attempts = fail_item(
                        ops, item["id"], error_text,
                        backoff=_exponential_backoff(_item_attempts(ops, item["id"])),
                    )
                    logger.warning(
                        "Rate limited on %s (attempt %d) — rescheduled",
                        item["_post_id"], attempts,
                    )
                else:
                    attempts = fail_item(ops, item["id"], error_text, backoff=0)

                # Dead letter check
                if attempts >= MAX_ATTEMPTS:
                    _dead_letter_insert(
                        ops, item["_post_id"], item["_domain"], error_text, attempts,
                    )
                    logger.error(
                        "Post %s moved to dead_letter after %d attempts: %s",
                        item["_post_id"], attempts, error_text,
                    )

                total_failed += 1
                processed_count += 1

    return {"processed": total_processed, "failed": total_failed}


def post_materialization(
    ops: SQLiteResource,
    job_id: int,
    processed: int,
    failed: int,
    dagster_url: str = _DAGSTER_URL,
) -> bool:
    """POST asset materialization event to Dagster.

    Returns True if the POST succeeded (200), False otherwise.
    Non-fatal on failure — the data is already persisted.
    """
    progress = batch_progress(ops, job_id)
    payload = json.dumps({
        "asset_key": "gold_analyses",
        "metadata": {
            "batch_id": job_id,
            "items_processed": processed,
            "items_failed": failed,
            "batch_total": progress["total"],
            "batch_processed": progress["processed"],
            "batch_failed": progress["failed"],
            "batch_pending": progress["pending"],
        },
    }).encode("utf-8")

    url = f"{dagster_url}/report_asset_materialization/"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("POSTed materialization to Dagster (200)")
                return True
            logger.warning(
                "Dagster POST returned %d: %s",
                resp.status, resp.read().decode(errors="replace")[:200],
            )
            return False
    except Exception as exc:
        logger.warning("Failed to POST to Dagster: %s", exc)
        return False


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Standalone enrichment worker — processes batch items via Gemini."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show batch state without processing",
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        default=None,
        help="Process a specific batch by ID",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N items (default: all)",
    )
    parser.add_argument(
        "--dagster-url",
        default=_DAGSTER_URL,
        help=f"Dagster base URL (default: {_DAGSTER_URL})",
    )
    args = parser.parse_args()
    dagster_url = args.dagster_url


    ops = SQLiteResource()
    duckdb = DuckDBResource(database="data/state.duckdb")
    gemini = GeminiResource()

    if args.dry_run:
        # Show all pending batches
        from datalake.defs.enrichment.batch import _ensure_schema

        _ensure_schema(ops)
        conn = ops.get_connection()
        try:
            batches = conn.execute(
                "SELECT id, status, total_items, processed_items, failed_items "
                "FROM batch_jobs WHERE status != 'complete' ORDER BY id"
            ).fetchall()

            if not batches:
                logger.info("No active batches found.")
                return

            for b in batches:
                logger.info(
                    "Batch %d: status=%s total=%d processed=%d failed=%d",
                    b[0], b[1], b[2], b[3], b[4],
                )
        finally:
            conn.close()
        return

    # Claim or use specified batch
    if args.batch_id:
        batch = {"id": args.batch_id, "payloads": [], "consumer": "gemini"}
        # Load items for the specified batch
        from datalake.defs.enrichment.batch import _ensure_schema

        _ensure_schema(ops)
        conn = ops.get_connection()
        try:
            items = conn.execute(
                "SELECT payload FROM batch_items WHERE job_id = ? ORDER BY id",
                [args.batch_id],
            ).fetchall()
            if not items:
                logger.error("Batch %d not found or has no items.", args.batch_id)
                sys.exit(1)
            batch["payloads"] = [r[0] for r in items]
        finally:
            conn.close()
    else:
        logger.info("Looking for pending batch...")
        batch = claim_batch(ops, consumer="gemini")
        if not batch:
            logger.info("No pending batches found.")
            return

        logger.info(
            "Claimed batch %d with %d items",
            batch["id"], len(batch["payloads"]),
        )

    # Process
    result = process_batch(
        ops, duckdb, gemini, batch,
        limit=args.limit,
        dry_run=False,
    )

    quota_hit = result.get("quota_exhausted", False)
    logger.info(
        "Batch %d complete: processed=%d failed=%d%s",
        batch["id"], result["processed"], result["failed"],
        " (quota exhausted — remaining items rescheduled)" if quota_hit else "",
    )

    # Mark complete only when nothing is left pending — quota exhaustion and
    # backoff-rescheduled items both leave pending work for a later run.
    progress = batch_progress(ops, batch["id"])
    remaining = progress["pending"] + progress["processing"]
    if remaining == 0:
        mark_complete(ops, batch["id"])
    else:
        logger.info(
            "Batch %d not marked complete — %d item(s) still pending%s.",
            batch["id"],
            remaining,
            " (quota exhausted)" if quota_hit else " (backoff)",
        )
    post_materialization(ops, batch["id"], result["processed"], result["failed"], dagster_url)


if __name__ == "__main__":
    main()
