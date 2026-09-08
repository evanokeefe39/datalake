"""Bounded pilot for the universal video→Gemini call (US-EFAC-3 + US-ESUM-1).

Runs the ONE universal media call (visual facets + content_summary + folded
per-image summaries) over a BOUNDED slice of media-bearing silver posts —
SMOKE (1 post) or PILOT (<=30) — writing ONLY to an isolated pilot DuckDB
(``data/facets_pilot.duckdb``). Never touches ``data/state.duckdb``; prod
``ops.sqlite`` is touched only through the existing scrape-time byte-cache /
File-API path (``_resolve_media_for_post``), which is additive + idempotent.
Dead-letter rows land in the PILOT DB, not prod.

Per-item backpressure mirrors ``process_item``: exponential backoff with
jitter, 429 taxonomy (rate-limit burst -> retry; RPD quota exhausted -> abort
run), MAX_ATTEMPTS then dead-letter. Cost accounting (est. + actual token
usage) is printed at start and end; per-item failures never abort the run.

SPEND BOUNDARY: this script hard-caps at --limit posts. NEVER run the full
corpus with it (design §7: full run ≈ $9–18, needs its own approval).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.enrichment import analysis as ew  # noqa: E402
from datalake.defs.enrichment.batch import MAX_ATTEMPTS, _now_iso  # noqa: E402
from datalake.defs.enrichment.facets import (  # noqa: E402
    UNIVERSAL_MAX_OUTPUT_TOKENS,
    run_universal_call,
    write_gold_facets_conn,
)
from datalake.defs.enrichment.prompts import (  # noqa: E402
    _DEFAULT_GEMINI_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    build_growth_facets_prompt,
)
from datalake.defs.instagram.config import GeminiTierConfig  # noqa: E402
from scripts.facet_experiment import _tag_media, sample_posts  # noqa: E402

logger = logging.getLogger("enrich_facets_pilot")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"
DEFAULT_PILOT_DB = "data/facets_pilot.duckdb"

# Rough pre-flight estimate: design §7 ~$0.002/media (low-res video input
# dominates); text-only fallback items cost ~caption only.
EST_COST_PER_POST_USD = 0.002


def _init_pilot_db(path: str) -> "duckdb.DuckDBPyConnection":  # type: ignore[name-defined]
    import duckdb

    conn = duckdb.connect(path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS facets_pilot (
               post_id VARCHAR PRIMARY KEY,
               media_type VARCHAR,
               n_media INTEGER,
               ok BOOLEAN,
               error TEXT,
               growth_facets_json TEXT,
               content_summary TEXT,
               image_summaries_json TEXT,
               usage_json TEXT,
               cost_usd DOUBLE,
               elapsed_s DOUBLE,
               model VARCHAR,
               prompt_hash VARCHAR,
               raw_response TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS pilot_dead_letter (
               post_id VARCHAR PRIMARY KEY,
               error TEXT,
               attempts INTEGER,
               failed_at TEXT
           )"""
    )
    return conn


def _save_result(conn, post: dict, rec: dict, model: str) -> None:
    usage = rec.get("usage")
    conn.execute(
        """INSERT OR REPLACE INTO facets_pilot VALUES
           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            post["post_id"],
            post.get("media_type"),
            rec.get("n_media"),
            rec.get("ok"),
            "; ".join(rec.get("errors") or []) or None,
            json.dumps(rec["visual_facets"], sort_keys=True)
            if rec.get("visual_facets") is not None else None,
            rec.get("content_summary"),
            json.dumps(rec.get("image_summaries"))
            if rec.get("image_summaries") is not None else None,
            json.dumps(usage) if usage else None,
            rec.get("cost_usd"),
            rec.get("elapsed_s"),
            model,
            CURRENT_FACETS_PROMPT_HASH,
            rec.get("raw"),
        ],
    )


def _dead_letter(conn, post: dict, error: str, attempts: int) -> None:
    logger.warning("dead-letter %s after %d attempts: %s",
                   post["post_id"], attempts, error[:200])
    conn.execute(
        "INSERT OR REPLACE INTO pilot_dead_letter VALUES (?, ?, ?, ?)",
        [post["post_id"], error, attempts, _now_iso()],
    )


def _process_with_backpressure(
    ops: SQLiteResource,
    gemini: GeminiResource,
    post: dict,
    pilot_conn,
) -> dict:
    """One post through the universal call with the worker's 429 taxonomy.

    Mirrors ``process_item``: rate-limit burst -> backoff + retry; RPD quota
    exhausted -> raise QuotaExhausted (caller stops the run); media/validation
    failures retry with backoff then dead-letter. Returns the last result rec.
    """
    last: dict = {"ok": False, "errors": ["not attempted"]}
    for attempt in range(MAX_ATTEMPTS):
        try:
            rec = run_universal_call(
                ops, gemini, post["post_id"], post["caption"] or "",
                post["media_files"],
            )
        except Exception as exc:  # noqa: BLE001 — per-item isolation
            error_text = f"{type(exc).__name__}: {exc}"
            text = str(exc)
            if ew._is_quota_exhausted(exc, text):
                raise QuotaExhausted(post["post_id"], text) from exc
            if ew._is_rate_limited(exc, text):
                backoff = ew._exponential_backoff(attempt)
                logger.info("rate-limited %s — backoff %.1fs",
                            post["post_id"], backoff)
                time.sleep(backoff)
                last = {"ok": False, "errors": [f"rate limited: {text}"]}
                continue
            # File-API / media / transient errors: retry with backoff too.
            backoff = ew._exponential_backoff(attempt)
            logger.info("post %s attempt %d failed (%s) — backoff %.1fs",
                        post["post_id"], attempt + 1,
                        ew._is_file_api_error(exc, text) and "file-api" or "api",
                        backoff)
            time.sleep(backoff)
            last = {"ok": False, "errors": [text]}
            continue

        if rec.get("ok"):
            return rec
        if rec.get("text_only"):
            # Terminal per-item condition — no retry can fix a dead CDN URL.
            _dead_letter(pilot_conn, post,
                         "; ".join(rec["errors"]), attempt + 1)
            return rec
        # Validation failure: JSON truncation/parse issues can be transient;
        # a schema mismatch is not — record raw text for inspection either way.
        logger.info("post %s invalid output (attempt %d): %s",
                    post["post_id"], attempt + 1, rec["errors"][:1])
        last = rec
        time.sleep(ew._exponential_backoff(0))
    _dead_letter(pilot_conn, post, "; ".join(last.get("errors") or []),
                 MAX_ATTEMPTS)
    return last


class QuotaExhausted(RuntimeError):
    def __init__(self, post_id: str, message: str):
        super().__init__(f"RPD quota exhausted at post {post_id}: {message}")




def run_pilot(args: argparse.Namespace) -> int:
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set")
        return 1
    tier = GeminiTierConfig.detect()
    if not tier.supports_video:
        print(f"tier {tier.tier} does not support video — universal call needs Tier-1+")
        return 1

    ops = SQLiteResource(database=str(REPO_ROOT / args.ops_db))
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])

    mode = "smoke" if args.smoke else "pilot"
    limit = 1 if args.smoke else args.limit
    targets = {"video": args.video, "carousel": args.carousel}
    posts = sample_posts(str(REPO_ROOT / args.state_db), limit, args.seed,
                         targets=targets)
    if not posts:
        print("no media-bearing posts selected")
        return 1

    # Pre-flight cost accounting (AC5).
    est = len(posts) * EST_COST_PER_POST_USD
    print(f"[{mode}] {len(posts)} media-bearing posts "
          f"(video={sum(1 for p in posts if p['media_type'] == 'video')}, "
          f"carousel={sum(1 for p in posts if p['media_type'] == 'carousel')}) "
          f"model={_DEFAULT_GEMINI_MODEL} "
          f"media_resolution=MEDIA_RESOLUTION_LOW "
          f"max_output_tokens={UNIVERSAL_MAX_OUTPUT_TOKENS}")
    print(f"[{mode}] ESTIMATED cost ≈ ${est:.4f} "
          f"(~$0.002/post, design §7); wall-time est "
          f"{len(posts) * 8}–{len(posts) * 20}s incl. uploads")

    pilot_conn = _init_pilot_db(str(REPO_ROOT / args.pilot_db))

    ok_n = dead_n = quota_stop = 0
    total_cost = 0.0
    started = time.monotonic()
    for post in posts:
        try:
            rec = _process_with_backpressure(ops, gemini, post, pilot_conn)
        except QuotaExhausted as exc:
            print(f"STOP: {exc}")
            quota_stop = 1
            break
        _save_result(pilot_conn, post, rec, _DEFAULT_GEMINI_MODEL)
        total_cost += rec.get("cost_usd") or 0.0
        if rec.get("ok"):
            ok_n += 1
            if not rec.get("text_only") and rec.get("visual_facets"):
                write_gold_facets_conn(
                    pilot_conn, post["post_id"], "instagram",
                    rec["visual_facets"], rec.get("content_summary"),
                    rec.get("image_summaries"),
                    model=_DEFAULT_GEMINI_MODEL,
                )
        else:
            dead_n += 1
    wall = time.monotonic() - started

    summary = pilot_conn.execute(
        "SELECT ok, count(*) FROM facets_pilot GROUP BY ok"
    ).fetchall()
    print(f"[{mode}] done: {ok_n} ok, {dead_n} failed/dead-lettered "
          f"(quota_stop={quota_stop}); wall={wall:.1f}s; "
          f"actual usage-based cost=${total_cost:.4f}")
    print(f"[{mode}] pilot DB rows by ok: {summary}")
    pilot_conn.close()
    return 1 if quota_stop else 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--smoke", action="store_true",
                   help="External-Integration-Gate smoke: exactly 1 post")
    p.add_argument("--limit", type=int, default=30,
                   help="max posts for the pilot slice (hard cap; <=30 approved)")
    p.add_argument("--video", type=int, default=15, dest="video")
    p.add_argument("--carousel", type=int, default=15, dest="carousel")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--state-db", default=DEFAULT_STATE_DB)
    p.add_argument("--ops-db", default=DEFAULT_OPS_DB)
    p.add_argument("--pilot-db", default=DEFAULT_PILOT_DB)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    return run_pilot(_parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
