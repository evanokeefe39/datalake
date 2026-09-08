"""Full-corpus production driver for the universal video→Gemini call
(US-EFAC-3 + US-ESUM-1) — writes ``gold_growth_facets`` into
``data/state.duckdb`` (the production gold DB, NOT the scratch pilot DB).

Reuses the proven pieces from the bounded pilot:
- ``facets.run_universal_call``  — byte cache → File API → one flash-lite call
  at MEDIA_RESOLUTION_LOW with the 4096-token universal output budget;
- the pilot's 429/backpressure taxonomy (rate-limit → backoff+retry,
  RPD-quota → stop the run, media/validation failures → backoff then
  dead-letter);
- ``facets.write_gold_facets_conn`` — the ordering-guarded gold upsert.

Production-specific behavior:
- **Resume-safe**: a post already in ``gold_growth_facets`` under the CURRENT
  facets prompt hash is skipped; a re-run only processes the remainder.
  Stale-prompt rows re-enqueue (same semantics as gold_analyses re-enrichment).
- **Media-unavailable is TERMINAL**: a post whose media is not fully present
  in the scrape-time byte cache (``ops.sqlite media_cache``) is dead-lettered
  once as terminal and skipped — never retried 5x (deferred remediation,
  issue #25: re-scrape/cache backfill).
- **--plan (dry-run)**: prints the projected eligible post count and cost
  under BOTH estimates — design §7 (~$0.002/media) and the measured pilot
  average (~$0.00092/media) — without any Gemini call or spend.

Usage:
  uv run python scripts/enrich_facets_full.py --plan
  uv run python scripts/enrich_facets_full.py --limit 5          # bounded smoke
  uv run python scripts/enrich_facets_full.py --post-ids 123,456
  uv run python scripts/enrich_facets_full.py --owners bywaviboy
  uv run python scripts/enrich_facets_full.py                    # full corpus
"""

from __future__ import annotations

import argparse
import json
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
from datalake.defs.enrichment.facets import (  # noqa: E402
    UNIVERSAL_MAX_OUTPUT_TOKENS,
    run_universal_call,
    write_gold_facets_conn,
)
from datalake.defs.enrichment.media_cache import url_hash  # noqa: E402
from datalake.defs.enrichment.prompts import (  # noqa: E402
    _DEFAULT_GEMINI_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
)
from datalake.defs.instagram.config import GeminiTierConfig  # noqa: E402

logger = logging.getLogger("enrich_facets_full")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"

# Cost estimates per media-bearing post, USD (shown together in --plan):
#   design §7 pre-flight estimate  ~$0.002/media (low-res video input dominates)
#   measured pilot average         ~$0.00092/media (usage-based, facets pilot)
EST_COST_PER_POST_DESIGN_USD = 0.002
EST_COST_PER_POST_PILOT_USD = 0.00092

# Wall-time estimate per post incl. File API uploads (pilot observation).
EST_SECONDS_PER_POST = (8, 20)


def _media_urls(media_files_json: str | None) -> list[str]:
    try:
        urls = json.loads(media_files_json) if media_files_json else []
    except (json.JSONDecodeError, TypeError):
        return []
    return [u for u in urls if isinstance(u, str) and u.strip()]


def _cached_urls(ops_path: str, urls: list[str]) -> set[str]:
    """Return the subset of ``urls`` whose bytes are in the scrape-time cache
    AND whose local files still exist. Read-only over ops.sqlite."""
    if not urls:
        return set()
    hashes = [url_hash(u) for u in urls]
    conn = sqlite3.connect(f"file:{ops_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            f"SELECT cache_key, local_path FROM media_cache "
            f"WHERE cache_key IN ({','.join('?' * len(hashes))})",
            hashes,
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()
    by_hash = {r[0]: r[1] for r in rows}
    return {
        u for u, h in zip(urls, hashes)
        if by_hash.get(h) and os.path.exists(by_hash[h])
    }


def enumerate_targets(
    state_db: str, ops_db: str,
    post_ids: list[str] | None = None, owners: list[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Split media-bearing silver posts into (eligible, media_unavailable).

    Eligible = media fully cached (every URL has live cached bytes). Anything
    else with media is terminal media-unavailable (issue #25) — reported, not
    retried. Read-only over state.duckdb.
    """
    import duckdb

    con = duckdb.connect(state_db, read_only=True)
    sql = ("SELECT post_id, caption, media_files, owner_username "
           "FROM silver_ig_posts WHERE media_count > 0")
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
    unavailable: list[dict] = []
    for pid, caption, mf, owner in rows:
        if not (caption or "").strip():
            continue  # empty captions skipped at the label pass (US-L6)
        post = {"post_id": pid, "caption": caption or "",
                "media_files": mf, "owner_username": owner}
        urls = _media_urls(mf)
        if urls and _cached_urls(ops_db, urls) == set(urls):
            eligible.append(post)
        else:
            unavailable.append(post)
    return eligible, unavailable


def _done_post_ids(state_db: str) -> set[str]:
    """Posts already materialized under the CURRENT facets prompt hash."""
    import duckdb

    con = duckdb.connect(state_db, read_only=True)
    try:
        rows = con.execute(
            "SELECT post_id FROM gold_growth_facets WHERE prompt_hash = ?",
            [CURRENT_FACETS_PROMPT_HASH],
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


def _process_with_backpressure(
    ops: SQLiteResource,
    gemini: GeminiResource,
    post: dict,
    state_conn,
    ops_path: str,
) -> dict:
    """One post through the universal call with the worker's 429 taxonomy.

    Mirrors the pilot: rate-limit → backoff + retry; RPD quota exhausted →
    raise QuotaExhausted (caller stops the run); media/validation failures →
    backoff then dead-letter. Media-unavailable is pre-filtered upstream and
    never reaches here.
    """
    last: dict = {"ok": False, "errors": ["not attempted"]}
    for attempt in range(MAX_ATTEMPTS):
        try:
            rec = run_universal_call(
                ops, gemini, post["post_id"], post["caption"],
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
            backoff = ew._exponential_backoff(attempt)
            logger.info("post %s attempt %d failed (%s) — backoff %.1fs",
                        post["post_id"], attempt + 1,
                        ew._is_file_api_error(exc, text) and "file-api" or "api",
                        backoff)
            time.sleep(backoff)
            last = {"ok": False, "errors": [text]}
            continue

        if rec.get("ok"):
            write_gold_facets_conn(
                state_conn, post["post_id"], "instagram",
                rec["visual_facets"], rec.get("content_summary"),
                rec.get("image_summaries"),
                model=_DEFAULT_GEMINI_MODEL,
            )
            return rec
        if rec.get("text_only"):
            # Terminal per-item condition — no retry can fix a dead CDN URL.
            _dead_letter(ops_path, post["post_id"],
                         "; ".join(rec["errors"]), attempt + 1)
            return rec
        # Validation failure: JSON truncation/parse issues can be transient.
        logger.info("post %s invalid output (attempt %d): %s",
                    post["post_id"], attempt + 1, rec["errors"][:1])
        last = rec
        time.sleep(ew._exponential_backoff(0))
    _dead_letter(ops_path, post["post_id"],
                 "; ".join(last.get("errors") or []), MAX_ATTEMPTS)
    return last


class QuotaExhausted(RuntimeError):
    def __init__(self, post_id: str, message: str):
        super().__init__(f"RPD quota exhausted at post {post_id}: {message}")


def run_plan(args: argparse.Namespace) -> int:
    eligible, unavailable = enumerate_targets(
        args.state_db, args.ops_db,
        _csv(args.post_ids), _csv(args.owners),
    )
    done = _done_post_ids(args.state_db)
    remaining = [p for p in eligible if p["post_id"] not in done]

    lo_s, hi_s = EST_SECONDS_PER_POST
    print(f"[plan] media-bearing posts with caption: "
          f"{len(eligible) + len(unavailable)}")
    print(f"[plan] eligible (media fully byte-cached): {len(eligible)}")
    print(f"[plan] media-unavailable (terminal, issue #25): {len(unavailable)}")
    print(f"[plan] already materialized (current prompt_hash): "
          f"{len(eligible) - len(remaining)}")
    print(f"[plan] remaining to process: {len(remaining)}")
    print(f"[plan] cost projection:")
    print(f"[plan]   design §7 estimate  "
          f"~$0.002/post     → ${len(remaining) * EST_COST_PER_POST_DESIGN_USD:.2f}")
    print(f"[plan]   pilot measured     "
          f"~$0.00092/post   → ${len(remaining) * EST_COST_PER_POST_PILOT_USD:.2f}")
    print(f"[plan] wall-time estimate: {len(remaining) * lo_s // 60}–"
          f"{max(1, len(remaining) * hi_s // 60)} min incl. uploads")
    print(f"[plan] model={_DEFAULT_GEMINI_MODEL} "
          f"media_resolution=MEDIA_RESOLUTION_LOW "
          f"max_output_tokens={UNIVERSAL_MAX_OUTPUT_TOKENS} "
          f"prompt_hash={CURRENT_FACETS_PROMPT_HASH}")
    if unavailable:
        print(f"[plan] sample media-unavailable post_ids: "
              f"{[p['post_id'] for p in unavailable[:5]]}")
    print("[plan] dry-run — no Gemini calls made, no spend.")
    return 0


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def run_full(args: argparse.Namespace) -> int:
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set")
        return 1
    tier = GeminiTierConfig.detect()
    if not tier.supports_video:
        print(f"tier {tier.tier} does not support video — universal call needs Tier-1+")
        return 1
    state_db = str(REPO_ROOT / args.state_db)
    ops_path = str(REPO_ROOT / args.ops_db)
    _ensure_gold_facets(state_db)  # additive, idempotent

    eligible, unavailable = enumerate_targets(
        state_db, ops_path, _csv(args.post_ids), _csv(args.owners),
    )
    done = _done_post_ids(state_db)
    posts = [p for p in eligible if p["post_id"] not in done]
    if args.limit is not None:
        posts = posts[: args.limit]

    print(f"[full] eligible={len(eligible)} "
          f"already_done={len(eligible) - len(posts)} "
          f"remaining_this_run={len(posts)} "
          f"media_unavailable_terminal={len(unavailable)}")
    for p in unavailable:
        # Terminal classification, NOT retried (issue #25 deferred remediation).
        _dead_letter(ops_path, p["post_id"],
                     "media unavailable: media_files not fully present in "
                     "scrape-time byte cache", 1)

    if not posts:
        print("[full] nothing to do")
        return 0

    ops = SQLiteResource(database=ops_path)
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])
    import duckdb

    state_conn = duckdb.connect(state_db)
    state_conn.execute(duckdb_ddl("gold_growth_facets"))

    ok_n = dead_n = quota_stop = 0
    total_cost = 0.0
    started = time.monotonic()
    for i, post in enumerate(posts):
        try:
            rec = _process_with_backpressure(ops, gemini, post, state_conn, ops_path)
        except QuotaExhausted as exc:
            print(f"STOP: {exc}")
            quota_stop = 1
            break
        total_cost += rec.get("cost_usd") or 0.0
        if rec.get("ok"):
            ok_n += 1
        else:
            dead_n += 1
        if (i + 1) % 25 == 0 or i + 1 == len(posts):
            wall = time.monotonic() - started
            print(f"[full] {i + 1}/{len(posts)} ok={ok_n} dead={dead_n} "
                  f"cost=${total_cost:.4f} wall={wall:.0f}s")
    wall = time.monotonic() - started
    state_conn.close()

    print(f"[full] done: {ok_n} ok, {dead_n} failed/dead-lettered "
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
