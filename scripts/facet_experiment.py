"""Facet-list schema-selection experiment (see tasks/plans/facet-list-experiment-design.md).

Drives the OPEN-extraction growth-facet schema over a stratified media sample
through the Gemini BATCH API, writing results to a TEMP DuckDB. It deliberately
does NOT touch production state: no gold_analyses writes, no ops.sqlite
batch_items orchestration, no use of IG_GOLD_PROMPT / CURRENT_PROMPT_HASH.
Media bytes are resolved through the shared scrape-time cache (additive).

Two commands:
  run     build requests (facet prompt + caption + resolved media) and submit
          one Gemini batch job per run-index, poll to completion, store into
          <out>.duckdb table `facet_results(post_id, run, result_json, ...)`.
  analyze load <out>.duckdb, print per-field inter-run agreement + NA/null rates.

Usage (tier1 env in .env; low-res media; ~$0.002 per media call):
  uv run python scripts/facet_experiment.py run   --sample 3  --runs 1
  uv run python scripts/facet_experiment.py run   --sample 150 --runs 3   # D1 agreement
  uv run python scripts/facet_experiment.py run   --sample 150 --runs 1 --no-media  # caption control
  uv run python scripts/facet_experiment.py analyze
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from datalake.defs.common.resources import GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.enrichment import gemini_batch  # noqa: E402
from datalake.defs.enrichment.media_cache import lookup_or_upload_all  # noqa: E402
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL  # noqa: E402

logger = logging.getLogger("facet_experiment")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"
DEFAULT_OUT = "data/facet_experiment.duckdb"

FACET_JSON_SCHEMA_HINT = """\
Return ONLY valid JSON. No markdown, no explanation.

Field contract:
- face_present: boolean (does the media show a human face)
- original_audio: boolean (does audio sound original/creator-spoken vs licensed/trending)
- hook_present: boolean (an attention-grabbing opening in the first ~3s / caption open)
- hook_type: STRING or null. null when hook_present is false. NOT an enum — name the
  natural hook type. Example possibilities (guidance only, never exhaustive): question,
  bold claim, curiosity gap, pattern interrupt, personal story, contrarian take,
  shocking stat, provocative statement, "watch until the end", direct value promise.
- hook_content: STRING or null. null when hook_present is false. The opening line /
  on-screen text in the first ~3s, as verbatim as possible (max ~120 chars). If
  spoken-only, a terse paraphrase. No interpretation, no category words inside it.
- format_structure: STRING. Example possibilities (guidance only): tutorial, demo,
  case study, comparison, Q&A, behind-the-scenes, day-in-life, transformation,
  debunk/myth-bust, news/update, personal story, review, curated collection.
- value_medium: STRING. How value is conveyed. Example possibilities: live
  demonstration, talking-head explanation, screen recording, text-overlay slides,
  static illustration/photo, B-roll + voiceover, animation.
- visual_style: STRING. Example possibilities: photo-real, illustration, meme,
  slide-deck text, raw/first-person footage, green-screen, polished edit.
- text_overlay: STRING. Example possibilities: none, burned-in captions, title-only,
  dense on-screen text.
- originality: STRING. Example possibilities: fully original, trend-remix,
  adaptation of an idea, curated repost.
- visual_quality: one of "lo-fi", "clean", "high-production" (a scale, pick closest).
- value_depth: one of "shallow", "practical", "deep" (a scale: shallow=entertainment,
  practical=actionable tips, deep=substantive teaching). Pick closest.
- is_sponsored: boolean (paid promotion / brand deal / affiliate/product push with
  a promotional intent, whether or not formally disclosed)
- sponsorship_signal: STRING or null. If sponsored, the cue (e.g. "disclosed #ad",
  "affiliate code", "product placement") else null.
- claimed_results: STRING or null. If the content states a specific result/outcome
  number or guarantee (e.g. "grew to 100k in 30 days", "guaranteed"), quote it
  verbatim (~60 chars) else null.
- brand_safety_flags: object with booleans: competitor_logo (another brand's logo/
  mark clearly shown), profanity (strong profanity in audio/text), sensitive_adjacency
  (touchy subject — politics, health claims, finance promises, trauma)
- audience_named: boolean (does the caption name the intended viewer, e.g. "for
  freelancers", "if you're a founder")
- cta_type: STRING or null. Calls to action in caption/on-screen. Example
  possibilities (guidance only): none, comment, save, share, follow, link-in-bio,
  DM, download, subscribe.
- hashtag_strategy: STRING or null. Observation of the hashtag set. Example
  possibilities: broad+niche mix, niche-only, no hashtags, brand/community tag,
  keyword-stuffed.
- replicable_tactic: STRING or null. The single most reusable tactic a viewer could
  copy from this post, in one short line (~100 chars) or null if none.

Rule: hook_type and hook_content are BOTH null if and only if hook_present is false.
Do not invent fields beyond the above. Do NOT output is_educational, is_actionable,
admiralty, domain, subdomain, content_type, format, educational_json, actionable_json.
"""

FACET_PROMPT = """\
You are a meticulous content analyst reading an Instagram post AND its attached
media (images/video). Extract structured growth + audit facets from what the
media and caption actually show — ground every judgment in the content, not in
general assumptions about the account.

""" + FACET_JSON_SCHEMA_HINT

# Cheap keyword pre-scan to over-sample rare classes (sponsorship / claims /
# competitor / face) so the sample carries enough positives for recall.
_RARE_PATTERNS = {
    "sponsor": re.compile(r"#ad\b|#sponsored\b|paid partnership|paid promotion|\bsponsored\b|discount code|use code|promo code|affiliate|\bgifted\b|link in bio|check out .*com\b", re.I),
    "claim": re.compile(r"guarantee|guaranteed|in \d+ days|grew .* to |went from .* to |100k|1m\b|results may vary", re.I),
    "competitor": re.compile(r"\bvs\b|better than|alternative to|switched (from|to) (Adobe|Canva|Notion|Figma|Shopify|Substack|Instagram|TikTok|YouTube)", re.I),
    "face": re.compile(r"talking|on camera|day in my life|morning routine|watch me|my face", re.I),
}


def _tag_media(mf: str | None) -> str:
    """Crude media-type tag from the media_files JSON for stratification."""
    if not mf or mf.strip() in ("", "[]", "null"):
        return "text"
    try:
        urls = json.loads(mf)
    except (json.JSONDecodeError, TypeError):
        return "text"
    if not urls:
        return "text"
    if any(".mp4" in u or ".mov" in u for u in urls):
        return "video"
    if len(urls) > 1:
        return "carousel"
    return "image"


def sample_posts(duck: str, n: int, seed: int,
                 targets: dict | None = None) -> list[dict]:
    """Balanced media-type sample + rare-signal preference within each type.

    Read-only over silver_ig_posts; text-only posts excluded. ``targets`` maps
    media_type -> how many to take; when None, uses balanced shares that
    over-represent the rare image/carousel types so per-media-type agreement
    is measurable: image 0.25 / carousel 0.30 / video 0.45. Within each type,
    posts whose caption matches a rare-signal pattern (sponsor/claim/
    competitor/face) are preferred first, then a random fill.
    """
    import duckdb

    random.seed(seed)
    con = duckdb.connect(duck, read_only=True)
    rows = con.execute(
        "SELECT post_id, caption, media_files FROM silver_ig_posts "
        "WHERE media_count > 0"
    ).fetchall()
    con.close()

    pools: dict[str, list[dict]] = {"video": [], "carousel": [], "image": []}
    for r in rows:
        pid, caption, mf = r[0], r[1] or "", r[2]
        if not caption.strip():
            continue
        mtype = _tag_media(mf)
        if mtype not in pools:
            continue
        pools[mtype].append({"post_id": pid, "caption": caption,
                             "media_files": mf, "media_type": mtype})

    targets = targets or {"image": max(1, round(0.25 * n)),
                          "carousel": max(1, round(0.30 * n)),
                          "video": max(1, round(0.45 * n))}
    picked: list[dict] = []
    for mtype, want in targets.items():
        pool = pools[mtype]
        rare = [x for x in pool if any(p.search(x["caption"])
                                       for p in _RARE_PATTERNS.values())]
        rest = [x for x in pool if x not in rare]
        random.shuffle(rare)
        random.shuffle(rest)
        chosen = rare[:max(0, want // 2)] + rest[: want - (want // 2)]
        if len(chosen) < want:
            chosen += rare[len(chosen):]
        picked.extend(chosen[:want])
    if len(picked) < n:
        have = {x["post_id"] for x in picked}
        spare = [x for pool in pools.values() for x in pool
                 if x["post_id"] not in have]
        random.shuffle(spare)
        for x in spare:
            if len(picked) >= n:
                break
            picked.append(x)
    return picked[:n]


def resolve_post_media(post: dict, ops: SQLiteResource,
                       gemini: GeminiResource) -> list:
    """Resolve a post's media to File-API/inline dicts once (shared across runs).

    Mirrors the production per-item resilience: a dead CDN URL / File-API
    failure for one post must NOT abort the run — return [] so the caller can
    drop the post (empty = unresolvable).
    """
    try:
        return lookup_or_upload_all(ops, gemini, post["media_files"],
                                    inline_images=True) or []
    except Exception as exc:  # noqa: BLE001 — dead media is a per-post failure
        logger.warning("dropping post %s (media unresolvable): %s",
                       post["post_id"], str(exc)[:120])
        return []


def build_request(post: dict, run: int, media_files: list,
                  include_media: bool) -> dict:
    """Build one batch request dict reusing resolved media (custom_key embeds run+post)."""
    custom_key = f"fe:{run}::{post['post_id']}"
    prompt = FACET_PROMPT + "\nCaption:\n" + post["caption"]
    req = {"custom_key": custom_key, "prompt": prompt, "post_id": post["post_id"],
           "run": run}
    if include_media and media_files:
        req["media_files"] = media_files
    return req


def _poll_until_done(gemini: GeminiResource, names: list[str],
                     timeout_s: int = 2400) -> dict[str, dict]:
    """Poll batch jobs to terminal state, then merge retrieved results."""
    from google.genai import Client

    client = Client(api_key=gemini.api_key)
    all_results: dict[str, dict] = {}
    done: set[int] = set()
    deadline = time.time() + timeout_s
    while len(done) < len(names):
        for i, name in enumerate(names):
            if i in done:
                continue
            job = client.batches.get(name=name)
            state = gemini_batch.job_state(job)
            if gemini_batch.is_terminal(state):
                done.add(i)
                try:
                    all_results.update(gemini_batch.retrieve(gemini, name))
                except Exception as exc:  # noqa: BLE001
                    logger.error("retrieve failed for %s: %s", name, exc)
        if len(done) < len(names):
            if time.time() > deadline:
                raise TimeoutError(f"batch jobs not terminal after {timeout_s}s")
            time.sleep(15)
    return all_results


def cmd_run(args) -> int:
    import duckdb

    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set in .env")
        return 1
    from datalake.defs.instagram.config import GeminiTierConfig
    if not GeminiTierConfig.detect().supports_batch:
        print("Batch API requires GEMINI_TIER=tier1/tier2 (paid key)")
        return 1

    ops = SQLiteResource(database=args.ops_db)
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])

    posts = sample_posts(args.state_db, args.sample, args.seed)
    if not posts:
        print("no posts sampled")
        return 1
    media_types = {p["post_id"]: p["media_type"] for p in posts}
    counts = {}
    for p in posts:
        counts[p["media_type"]] = counts.get(p["media_type"], 0) + 1
    print(f"sampled {len(posts)} media posts: {counts}")

    con = duckdb.connect(args.out)
    con.execute(
        """CREATE TABLE IF NOT EXISTS facet_results(
             post_id VARCHAR, run INTEGER, has_media BOOLEAN,
             result_json TEXT, ok BOOLEAN, error TEXT, media_type VARCHAR,
             PRIMARY KEY(post_id, run))"""
    )
    con.close()

    # Resolve media ONCE per post; drop unresolvable (dead-CDN) posts up front.
    resolved: dict[str, list] = {}
    live: list[dict] = []
    for p in posts:
        mf = resolve_post_media(p, ops, gemini) if args.media else []
        if args.media and not mf and p["media_files"]:
            logger.warning("post %s has no resolvable media — dropping", p["post_id"])
            continue
        resolved[p["post_id"]] = mf
        live.append(p)
    posts = live
    media_types = {p["post_id"]: p["media_type"] for p in posts}
    if not posts:
        print("all posts dropped on media resolution")
        return 1
    print(f"{len(posts)} posts with resolvable media after dead-URL filter")

    for run in range(args.runs):
        requests = [
            build_request(p, run, resolved[p["post_id"]], include_media=args.media)
            for p in posts
        ]
        names = gemini_batch.submit(gemini, _DEFAULT_GEMINI_MODEL, requests,
                                    f"facet-exp-r{run}-{len(requests)}")
        print(f"run {run}: submitted {len(names)} batch job(s) "
              f"({len(requests)} requests)")
        results = _poll_until_done(gemini, names)
        ok = sum(1 for r in results.values() if r["ok"])
        print(f"run {run}: {ok}/{len(requests)} ok")

        con = duckdb.connect(args.out)
        for key, rec in results.items():
            m = re.match(r"fe:(\d+)::(.+)", key)
            if not m:
                continue
            run_i, pid = int(m.group(1)), m.group(2)
            con.execute(
                "INSERT OR REPLACE INTO facet_results VALUES (?,?,?,?,?,?,?)",
                [pid, run_i, bool(args.media), rec.get("text"), rec.get("ok"),
                 rec.get("error"), media_types.get(pid)],
            )
        con.close()
    print(f"wrote {len(posts)} posts x {args.runs} runs to {args.out}")
    return 0


_FIELDS = [
    "face_present", "original_audio", "hook_present", "is_sponsored",
    "audience_named", "claimed_results", "hook_type", "hook_content",
    "format_structure", "value_medium", "visual_style", "text_overlay",
    "originality", "visual_quality", "value_depth", "sponsorship_signal",
    "cta_type", "hashtag_strategy", "replicable_tactic",
    "brand_safety_flags.competitor_logo", "brand_safety_flags.profanity",
    "brand_safety_flags.sensitive_adjacency",
]


def _get(obj: dict, dotted: str):
    cur = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _norm(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True)
    return str(v).strip().lower()


def cmd_analyze(args) -> int:
    import duckdb

    con = duckdb.connect(args.out, read_only=True)
    rows = con.execute(
        "SELECT post_id, run, result_json, media_type FROM facet_results "
        "ORDER BY post_id, run"
    ).fetchall()
    con.close()
    if not rows:
        print("no results yet")
        return 0

    by_post: dict[str, list[dict]] = {}
    for pid, run, rj, mtype in rows:
        try:
            obj = json.loads(rj) if rj else {}
        except json.JSONDecodeError:
            obj = {}
        by_post.setdefault(pid, []).append({"obj": obj, "media_type": mtype})

    print(f"posts: {len(by_post)} | runs/post: {sorted({len(v) for v in by_post.values()})}")
    print(f"{'field':34} {'pairwiseAgree%':>14} {'agreePairs':>10} {'NA%':>6}")
    for field in _FIELDS:
        agree_n = 0
        agree_tot = 0
        na_n = 0
        tot = 0
        for pid, outputs in by_post.items():
            vals = [_get(o["obj"], field) for o in outputs]
            tot += len(vals)
            na_n += sum(1 for v in vals if v is None)
            present = [v for v in vals if v is not None]
            for i in range(len(present)):
                for j in range(i + 1, len(present)):
                    agree_tot += 1
                    if _norm(present[i]) == _norm(present[j]):
                        agree_n += 1
        agree_pct = (100.0 * agree_n / agree_tot) if agree_tot else float("nan")
        print(f"{field:34} {agree_pct:14.1f} {agree_tot:10} {100.0 * na_n / tot:6.1f}")
    return 0


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--sample", type=int, default=3)
    r.add_argument("--runs", type=int, default=1)
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--no-media", action="store_true")
    r.add_argument("--state-db", default=DEFAULT_STATE_DB)
    r.add_argument("--ops-db", default=DEFAULT_OPS_DB)
    r.add_argument("--out", default=DEFAULT_OUT)
    a = sub.add_parser("analyze")
    a.add_argument("--out", default=DEFAULT_OUT)
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _parse_args(argv)
    args.media = not getattr(args, "no_media", False)
    if args.cmd == "run":
        args.ops_db = os.path.join(REPO_ROOT, args.ops_db)
        args.state_db = os.path.join(REPO_ROOT, args.state_db)
        args.out = os.path.join(REPO_ROOT, args.out)
        return cmd_run(args)
    args.out = os.path.join(REPO_ROOT, args.out)
    return cmd_analyze(args)


if __name__ == "__main__":
    sys.exit(main())
