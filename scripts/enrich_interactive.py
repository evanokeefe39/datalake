"""Out-of-band interactive enrichment tool (Phase 4, ADR-0007).

Standalone MANUAL developer tool — not orchestrated, not scheduled, not a
Dagster asset. Synchronously enriches a chosen subset of posts via Gemini
interactive mode and idempotently upserts ``gold_analyses`` (ordering-guarded
upsert on ``(post_id, domain, prompt_hash)``).

What it deliberately does NOT do:

- No batch claiming / rescheduling / status writes on ``batch_items`` — the
  orchestrated paths (batch submission + harvest run) own that coordination.
  Batch-id selection is READ-ONLY.
- No REST materialization POSTs to Dagster. Writes are recorded via logging
  (``GOLD UPSERT`` lines) so gold freshness stays honestly attributable to
  out-of-band runs; reconcile against the Dagster UI when needed.
- No silver writes. Silver stays hermetic (ADR-0008): Gemini API calls happen
  only here, at the interactive seam.

Reuse vs. reimplement: the per-post semantics (media resolution + tier/token
gates, prompt assembly, JSON validation, gold upsert, 429 taxonomy,
dead-letter routing) are IMPORTED from ``scripts/enrichment_worker.py`` —
nothing is duplicated. Only the loop around them differs: retries are local
(sleep + attempt counter) instead of queue-backed rescheduling, because this
tool does not participate in the batch queue.

Usage::

    uv run python scripts/enrich_interactive.py --post-ids p1,p2,p3
    uv run python scripts/enrich_interactive.py --batch-id 7      # read-only pick
    uv run python scripts/enrich_interactive.py --count 10        # drain-style pick
    uv run python scripts/enrich_interactive.py --count 5 --dry-run

Subset selection (exactly one of ``--post-ids`` / ``--batch-id`` / ``--count``):

- ``--post-ids``  explicit comma-separated post ids (domain defaults to instagram)
- ``--batch-id``  payloads of that job's still-open (pending/failed) items —
                  read-only selection, items are NOT claimed or completed
- ``--count``     drain-style discovery: label-approved posts (standout /
                  control / floor_filler, current label version) with no
                  current-prompt gold row and no open batch item

Exit: prints a summary (enriched / skipped / failed / dead-lettered) and the
exact post ids it upserted, so a manual run leaves an auditable record.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Make sibling-module import work regardless of how this file is invoked
# (direct script run, ``uv run``, or importlib in tests).
_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from dotenv import load_dotenv  # noqa: E402

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.enrichment.assets import ensure_gold_analyses  # noqa: E402
from datalake.defs.enrichment.batch import MAX_ATTEMPTS  # noqa: E402
from datalake.defs.instagram.labels import LABEL_VERSION  # noqa: E402
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH  # noqa: E402

load_dotenv()

# Reuse the worker's interactive enrichment semantics — single source of truth.
import enrichment_worker as ew  # noqa: E402

logger = logging.getLogger("enrich_interactive")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"

# Discovery query mirroring ig_posts_gen_batches' drain (label gate + gold +
# open batch items) — read-only; does not enqueue anything.
_LABEL_DRAIN_SQL = """
    SELECT l.post_id
    FROM ig_post_labels l
    WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
      AND l.label_version = ?
      AND NOT EXISTS (
          SELECT 1 FROM gold_analyses g
          WHERE g.post_id = l.post_id
            AND g.domain = ?
            AND g.prompt_hash = ?
      )
"""


def select_from_labels(
    duckdb: DuckDBResource,
    ops: SQLiteResource,
    count: int,
    domain: str = "instagram",
) -> list[str]:
    """Drain-style discovery of up to ``count`` unenriched, approved posts."""
    with duckdb.get_connection() as conn:
        candidates = [
            r[0]
            for r in conn.execute(
                _LABEL_DRAIN_SQL, [LABEL_VERSION, domain, CURRENT_PROMPT_HASH]
            ).fetchall()
        ]
    if candidates:
        ops_conn = ops.get_connection()
        try:
            open_ids = {
                (json.loads(r[0]) or {}).get("post_id")
                for r in ops_conn.execute(
                    "SELECT payload FROM batch_items "
                    "WHERE status IN ('pending', 'processing')"
                ).fetchall()
            }
        finally:
            ops_conn.close()
        candidates = [pid for pid in candidates if pid not in open_ids]
    return candidates[:count]


def select_from_batch(ops: SQLiteResource, job_id: int) -> list[str]:
    """Read-only selection: post ids from a job's still-open items.

    Does NOT claim, complete, reschedule, or otherwise mutate batch state.
    """
    ops_conn = ops.get_connection()
    try:
        rows = ops_conn.execute(
            "SELECT payload FROM batch_items "
            "WHERE job_id = ? AND status IN ('pending', 'failed') "
            "ORDER BY id",
            [job_id],
        ).fetchall()
    finally:
        ops_conn.close()
    return [
        pid
        for pid in ((json.loads(r[0]) or {}).get("post_id") for r in rows)
        if pid
    ]


def enrich_post(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    post_id: str,
    domain: str = "instagram",
) -> str:
    """Enrich one post synchronously and upsert gold. Returns 'enriched' or 'skipped'.

    Mirrors ``ew.process_item`` without any queue coordination: no claim,
    no ``complete_item``/``fail_item`` writes. Raises on Gemini/API errors —
    the caller owns the retry taxonomy.
    """
    table = ew._SILVER_TABLES.get(domain)
    if not table:
        raise ValueError(f"Unknown domain {domain!r} — no silver table mapping")

    with duckdb.get_connection() as conn:
        row = conn.execute(
            f"SELECT caption, media_files FROM {table} WHERE post_id = ?",
            [post_id],
        ).fetchone()

    if not row:
        logger.info("Post %s not in silver — skipped", post_id)
        return "skipped"

    caption = row[0] or ""
    if not caption.strip():
        logger.info("Post %s has empty caption — skipped", post_id)
        return "skipped"

    # Media: cache → File API upload, tier + token gated (reused worker logic)
    media_files = ew._resolve_media_for_post(ops, gemini, post_id, row[1])

    prompt_text = ew._PROMPTS.get(domain, ew.IG_GOLD_PROMPT) + "\n" + caption
    analyze_kwargs: dict = {}
    if media_files:
        analyze_kwargs["media_files"] = media_files
    result = gemini.analyze(prompt_text, **analyze_kwargs)

    try:
        json.loads(result)
    except json.JSONDecodeError:
        raise ValueError(f"Gemini returned invalid JSON for post {post_id}")

    ew._write_gold(duckdb, post_id, domain, result)
    logger.info(
        "GOLD UPSERT post_id=%s domain=%s prompt_hash=%s (out-of-band interactive run)",
        post_id,
        domain,
        CURRENT_PROMPT_HASH,
    )
    return "enriched"


def enrich_posts(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    post_ids: list[str],
    domain: str = "instagram",
) -> dict:
    """Enrich a subset with the worker's 429 taxonomy, minus queue coordination.

    - quota exhausted → halt the whole run immediately (nothing rescheduled)
    - rate limit / File API / other errors → bounded local retry with the
      worker's exponential backoff; at MAX_ATTEMPTS route to ``dead_letter``
      (the standard triage surface — keeps gold_analyses pure)

    Returns a summary dict: enriched / skipped / failed / dead_lettered ids,
    plus ``quota_exhausted`` when the run was halted.
    """
    ensure_gold_analyses(duckdb)

    enriched: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    dead_lettered: list[str] = []
    quota_exhausted = False

    for post_id in post_ids:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                outcome = enrich_post(ops, duckdb, gemini, post_id, domain)
                (enriched if outcome == "enriched" else skipped).append(post_id)
                break
            except Exception as exc:
                error_text = str(exc)

                if ew._is_quota_exhausted(exc, error_text):
                    reset_secs = ew._quota_reset_backoff()
                    logger.warning(
                        "Quota exhausted on %s — halting run. Quota resets in ~%ds.",
                        post_id,
                        reset_secs,
                    )
                    quota_exhausted = True
                    return {
                        "enriched": enriched,
                        "skipped": skipped,
                        "failed": failed,
                        "dead_lettered": dead_lettered,
                        "quota_exhausted": True,
                    }

                if attempt >= MAX_ATTEMPTS:
                    logger.error(
                        "Post %s failed after %d attempts — dead_letter: %s",
                        post_id,
                        attempt,
                        error_text[:200],
                    )
                    ew._dead_letter_insert(ops, post_id, domain, error_text, attempt)
                    dead_lettered.append(post_id)
                    failed.append(post_id)
                    break

                kind = (
                    "rate limit"
                    if ew._is_rate_limited(exc, error_text)
                    else (
                        "File API"
                        if ew._is_file_api_error(exc, error_text)
                        else "error"
                    )
                )
                backoff = ew._exponential_backoff(attempt)
                logger.warning(
                    "%s on %s (attempt %d/%d) — retrying in %.1fs: %s",
                    kind,
                    post_id,
                    attempt,
                    MAX_ATTEMPTS,
                    backoff,
                    error_text[:120],
                )
                time.sleep(backoff)

    return {
        "enriched": enriched,
        "skipped": skipped,
        "failed": failed,
        "dead_lettered": dead_lettered,
        "quota_exhausted": quota_exhausted,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Out-of-band interactive enrichment (manual tool — upserts gold_analyses)."
    )
    subset = parser.add_mutually_exclusive_group(required=True)
    subset.add_argument(
        "--post-ids", help="Comma-separated post ids to enrich explicitly."
    )
    subset.add_argument(
        "--batch-id",
        type=int,
        help="Pick still-open (pending/failed) items of this batch job. READ-ONLY selection.",
    )
    subset.add_argument(
        "--count",
        type=int,
        help="Drain-style pick: up to N label-approved, unenriched posts.",
    )
    parser.add_argument(
        "--domain", default="instagram", help="Enrichment domain (default: instagram)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the selected subset and exit — no API calls, no writes.",
    )
    parser.add_argument("--state-db", default=DEFAULT_STATE_DB)
    parser.add_argument("--ops-db", default=DEFAULT_OPS_DB)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parse_args(argv)

    if args.domain not in ew._SILVER_TABLES:
        logger.error("Unknown domain %r — known: %s", args.domain, sorted(ew._SILVER_TABLES))
        return 2

    duckdb = DuckDBResource(database=args.state_db)
    ops = SQLiteResource(database=args.ops_db)

    if args.post_ids:
        post_ids = [p.strip() for p in args.post_ids.split(",") if p.strip()]
        selection = "explicit --post-ids"
    elif args.batch_id is not None:
        post_ids = select_from_batch(ops, args.batch_id)
        selection = f"batch {args.batch_id} open items (read-only pick)"
    else:
        post_ids = select_from_labels(duckdb, ops, args.count, args.domain)
        selection = f"drain-style pick (count={args.count})"

    logger.info("Selected %d post(s) via %s: %s", len(post_ids), selection, post_ids)
    if not post_ids:
        logger.info("Nothing to enrich.")
        return 0
    if args.dry_run:
        logger.info("DRY RUN — no API calls, no writes.")
        return 0

    gemini = GeminiResource()
    summary = enrich_posts(ops, duckdb, gemini, post_ids, args.domain)

    logger.info(
        "Done — enriched: %d, skipped: %d, failed: %d, dead_lettered: %d%s",
        len(summary["enriched"]),
        len(summary["skipped"]),
        len(summary["failed"]),
        len(summary["dead_lettered"]),
        " (quota exhausted — run halted, re-run later)" if summary["quota_exhausted"] else "",
    )
    if summary["enriched"]:
        logger.info("Gold upserts by this run: %s", ", ".join(summary["enriched"]))
    return 1 if summary["quota_exhausted"] else 0


if __name__ == "__main__":
    sys.exit(main())
