"""Full-corpus driver for the TEXT-layer facet call (US-EFAC-4) — writes
merged text facets into ``gold_growth_facets`` in ``data/state.duckdb``.

The cheap, media-free companion to ``enrich_facets_full.py``: one TEXT-ONLY
flash-lite call per post over the caption (+ transcript when a transcript
channel exists). No media is uploaded or resolved; eligible = silver posts
with a non-empty caption. The extracted text-layer facets are validated by
``validate_text_facets`` and MERGED into the row's existing
``growth_facets_json`` (union of per-pass partials — see
``text_facets`` module docstring for the merge/validation contract).

Production behavior (mirrors enrich_facets_full.py):
- **Resume-safe**: posts already carrying the CURRENT text prompt_hash are
  skipped; stale-prompt rows re-enqueue.
- **429 taxonomy**: rate-limit → backoff+retry; RPD-quota → stop the run;
  repeated validation/parse failures → backoff then dead-letter.
- **--plan (dry-run)**: prints projected eligible post count + cost without
  any Gemini call or spend.

Usage:
  uv run python scripts/enrich_text_facets.py --plan
  uv run python scripts/enrich_text_facets.py --limit 5     # bounded smoke
  uv run python scripts/enrich_text_facets.py --post-ids 123,456
  uv run python scripts/enrich_text_facets.py --owners bywaviboy
  uv run python scripts/enrich_text_facets.py               # full corpus
  # DEFERRED until the universal run finishes (state.duckdb lock).
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from datalake.defs.common.resources import GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.common.schemas import duckdb_ddl  # noqa: E402
from datalake.defs.enrichment import analysis as ew  # noqa: E402
from datalake.defs.enrichment.batch import MAX_ATTEMPTS, _now_iso  # noqa: E402
from datalake.defs.enrichment.prompts import (  # noqa: E402
    _DEFAULT_GEMINI_MODEL,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
)
from datalake.defs.enrichment.text_facets import (  # noqa: E402
    TEXT_MAX_OUTPUT_TOKENS,
    run_text_call,
    write_text_facets_conn,
)
from datalake.defs.instagram.config import GeminiTierConfig  # noqa: E402

logger = logging.getLogger("enrich_text_facets")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"

# Cost estimates per post, USD. Text-only calls are tiny: caption+transcript
# prompt ≈ 500–2000 input tokens, output ≈ 400 tokens. Flash-lite list prices
# ($0.10/$0.40 per 1M) give ~$0.0002/post worst case; the universal media call
# for comparison is ~$0.002/post (design §7).
EST_COST_PER_POST_DESIGN_USD = 0.0002

# Wall-time estimate per text call (no media upload) — seconds.
EST_SECONDS_PER_POST = (2, 6)

PROGRESS_EVERY_N = 25


def enumerate_targets(
    state_db: str,
    post_ids: list[str] | None = None,
    owners: list[str] | None = None,
) -> list[dict]:
    """Silver posts with a non-empty caption — the text pass is media-free.

    Read-only over state.duckdb. Transcript is optional and read per-post at
    call time when a transcript channel exists (US-EFAC-4 AC4).
    """
    import duckdb

    con = duckdb.connect(state_db, read_only=True)
    sql = ("SELECT post_id, caption, owner_username "
           "FROM silver_ig_posts WHERE 1=1")
    params: list = []
    if post_ids:
        sql += f" AND post_id IN ({','.join('?' * len(post_ids))})"
        params.extend(post_ids)
    if owners:
        sql += f" AND owner_username IN ({','.join('?' * len(owners))})"
        params.extend(owners)
    rows = con.execute(sql, params).fetchall()
    con.close()

    eligible: list[dict] = []
    for pid, caption, owner in rows:
        if not (caption or "").strip():
            continue  # empty captions skipped (US-L6 label-pass convention)
        eligible.append({"post_id": pid, "caption": caption or "",
                         "owner_username": owner})
    return eligible


def _done_post_ids(state_db: str) -> set[str]:
    """Posts already materialized under the CURRENT text prompt hash."""
    import duckdb

    con = duckdb.connect(state_db, read_only=True)
    try:
        rows = con.execute(
            "SELECT post_id FROM gold_growth_facets WHERE prompt_hash = ?",
            [CURRENT_TEXT_FACETS_PROMPT_HASH],
        ).fetchall()
    except duckdb.CatalogException:
        rows = []
    finally:
        con.close()
    return {r[0] for r in rows}


def _ensure_gold_facets(state_db: str) -> None:
    """Idempotent, additive: create gold_growth_facets if absent."""
    import duckdb

    con = duckdb.connect(state_db)
    con.execute(duckdb_ddl("gold_growth_facets"))
    con.close()


def _dead_letter(ops_path: str, post_id: str, error: str, attempts: int) -> None:
    logger.warning("dead-letter %s after %d attempts: %s",
                   post_id, attempts, error[:200])
    conn = sqlite3.connect(ops_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO dead_letter (post_id, domain, error, "
            "attempts, failed_at) VALUES (?, 'instagram', ?, ?, ?)",
            [post_id, error, attempts, _now_iso()],
        )
        conn.commit()
    finally:
        conn.close()


class QuotaExhausted(RuntimeError):
    def __init__(self, post_id: str, message: str):
        super().__init__(f"RPD quota exhausted at post {post_id}: {message}")


def _process_with_backpressure(
    gemini: GeminiResource,
    post: dict,
    state_conn,
    ops_path: str,
) -> dict:
    """One post through the text call with the worker's 429 taxonomy."""
    last: dict = {"ok": False, "errors": ["not attempted"]}
    for attempt in range(MAX_ATTEMPTS):
        try:
            rec = run_text_call(gemini, post["post_id"], post["caption"])
        except Exception as exc:  # noqa: BLE001 — per-item isolation
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
            backoff = ew._exponential_backoff(attempt)
            logger.info("post %s attempt %d failed — backoff %.1fs",
                        post["post_id"], attempt + 1, backoff)
            time.sleep(backoff)
            last = {"ok": False, "errors": [text]}
            continue

        if rec.get("ok"):
            write_text_facets_conn(
                state_conn, post["post_id"], "instagram",
                rec["text_facets"], model=_DEFAULT_GEMINI_MODEL,
            )
            return rec
        # Validation failure: JSON truncation/parse issues can be transient.
        logger.info("post %s invalid output (attempt %d): %s",
                    post["post_id"], attempt + 1, rec["errors"][:1])
        last = rec
        time.sleep(ew._exponential_backoff(0))
    _dead_letter(ops_path, post["post_id"],
                 "; ".join(last.get("errors") or []), MAX_ATTEMPTS)
    return last


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def run_plan(args: argparse.Namespace) -> int:
    eligible = enumerate_targets(args.state_db, _csv(args.post_ids), _csv(args.owners))
    done = _done_post_ids(args.state_db)
    remaining = [p for p in eligible if p["post_id"] not in done]

    lo_s, hi_s = EST_SECONDS_PER_POST
    print(f"[plan] caption-bearing posts: {len(eligible)}")
    print(f"[plan] already materialized (current text prompt_hash): "
          f"{len(eligible) - len(remaining)}")
    print(f"[plan] remaining to process: {len(remaining)}")
    print(f"[plan] cost projection:")
    print(f"[plan]   text-call estimate  ~$0.0002/post → "
          f"${len(remaining) * EST_COST_PER_POST_DESIGN_USD:.2f}")
    print(f"[plan] wall-time estimate: {len(remaining) * lo_s // 60}–"
          f"{max(1, len(remaining) * hi_s // 60)} min (no media upload)")
    print(f"[plan] model={_DEFAULT_GEMINI_MODEL} media=NONE "
          f"max_output_tokens={TEXT_MAX_OUTPUT_TOKENS} "
          f"prompt_hash={CURRENT_TEXT_FACETS_PROMPT_HASH}")
    print("[plan] dry-run — no Gemini calls made, no spend.")
    return 0


def run_full(args: argparse.Namespace) -> int:
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set")
        return 1
    tier = GeminiTierConfig.detect()
    if not tier.supports_video:
        print(f"tier {tier.tier} does not support video — text pass assumed Tier-1+")
        return 1
    state_db = str(REPO_ROOT / args.state_db)
    ops_path = str(REPO_ROOT / args.ops_db)
    _ensure_gold_facets(state_db)  # additive, idempotent

    eligible = enumerate_targets(state_db, _csv(args.post_ids), _csv(args.owners))
    done = _done_post_ids(state_db)
    posts = [p for p in eligible if p["post_id"] not in done]
    if args.limit is not None:
        posts = posts[: args.limit]

    print(f"[text] eligible={len(eligible)} "
          f"already_done={len(eligible) - len(posts)} "
          f"remaining_this_run={len(posts)}")
    if not posts:
        print("[text] nothing to do")
        return 0

    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])
    import duckdb

    state_conn = duckdb.connect(state_db)
    state_conn.execute(duckdb_ddl("gold_growth_facets"))

    ok_n = dead_n = quota_stop = 0
    total_cost = 0.0
    started = time.monotonic()
    for i, post in enumerate(posts):
        try:
            rec = _process_with_backpressure(gemini, post, state_conn, ops_path)
        except QuotaExhausted as exc:
            print(f"STOP: {exc}")
            quota_stop = 1
            break
        total_cost += rec.get("cost_usd") or 0.0
        if rec.get("ok"):
            ok_n += 1
        else:
            dead_n += 1
        if (i + 1) % PROGRESS_EVERY_N == 0 or i + 1 == len(posts):
            wall = time.monotonic() - started
            print(f"[text] {i + 1}/{len(posts)} ok={ok_n} dead={dead_n} "
                  f"cost=${total_cost:.4f} wall={wall:.0f}s")
    wall = time.monotonic() - started
    state_conn.close()

    print(f"[text] done: {ok_n} ok, {dead_n} failed/dead-lettered "
          f"(quota_stop={quota_stop}); wall={wall:.1f}s; "
          f"actual usage-based cost=${total_cost:.4f}")
    return 1 if quota_stop else 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--plan", action="store_true",
                   help="dry-run: print projected count + cost, no Gemini calls")
    p.add_argument("--limit", type=int, default=None,
                   help="cap posts processed this run (bounded smoke)")
    p.add_argument("--post-ids", default=None,
                   help="comma-separated post_ids to target")
    p.add_argument("--owners", default=None,
                   help="comma-separated owner_usernames to target")
    p.add_argument("--state-db", default=DEFAULT_STATE_DB)
    p.add_argument("--ops-db", default=DEFAULT_OPS_DB)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    args = _parse_args(argv)
    return run_plan(args) if args.plan else run_full(args)


if __name__ == "__main__":
    sys.exit(main())
