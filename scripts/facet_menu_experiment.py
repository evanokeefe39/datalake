"""Facet-menu experiment: enum-vs-open (V0/V1) x codebook-density (V2) comparison.

Runs the SAME posts under three prompt presentations of the shared post-D1
facet schema and writes results to a TEMP DuckDB table `facet_menu(post_id,
variant, run, media_type, result_json, ok, error)`. Isolated: no gold/ops
writes. See tasks/plans/facet-list-experiment-design.md (panel agenda D1/A1).

  run     --variants V0,V1,V2 --sample 120 --runs 2
  analyze --out <db>   (per-field agreement/collapse/NA, per variant x media-type)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
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
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL  # noqa: E402
from scripts.facet_experiment import sample_posts, resolve_post_media  # noqa: E402

logger = logging.getLogger("facet_menu")

DEFAULT_OUT = "data/facet_menu.duckdb"

# ── Shared post-D1 facet schema (all variants forbid reserved gold keys) ──────

_RESERVED = ("Do NOT output is_educational, is_actionable, admiralty, domain, "
             "subdomain, content_type, format, educational_json, actionable_json. "
             "Return ONLY valid JSON.")

_OPEN_SCHEMA = """\
Analyze the Instagram post AND its attached media. Extract these facets, grounding
every value in what the media and caption actually show. Do not guess from the
account or generalize.

Booleans (true/false):
- face_present: is a human face shown in the media
- original_audio: audio is original/creator-spoken vs licensed/trending music
- is_sponsored: paid promotion / brand deal / affiliate or product push with
  promotional intent, disclosed or not
- audience_named: caption names the intended viewer (e.g. "for freelancers")
- brand_safety_risk.profanity: strong profanity in audio/on-screen/caption
- brand_safety_risk.sensitive_adjacency: sensitive subject (politics, unverified
  health/medical claims, finance promises/guarantees, trauma)
- has_brand_tag: caption uses a brand/community hashtag

Scale (pick the closest):
- value_depth: "shallow" (entertainment) | "practical" (actionable tips) | "deep"
  (substantive teaching/explanation)

Lists / extracts:
- brand_logos: array of the brand names / logos CLEARLY shown in the media
  (empty array if none). Just name what is visible; do not infer sponsorship.
- claimed_results: STRING or null. Only if the content states a specific OUTCOME
  or RESULT (a number achieved, a growth/income/health/financial result, or a
  guarantee). Quote it verbatim (~60 chars). Do NOT capture milestones, time
  stamps, or unrelated numbers.
- sponsorship_signal: STRING or null. If sponsored, the observable cue
  ("disclosed #ad", "affiliate/discount code", "product placement/name-drop",
  "review copy / gifted") else null.
- hook_content: STRING or null. The opening attention-grab in the first ~3s or
  caption open, as verbatim as possible (max ~120 chars); spoken-only -> terse
  paraphrase. No interpretation or category words inside the text.
- replicable_tactic: STRING or null. The single most reusable tactic a viewer
  could copy, one short line (~100 chars).
- evidence: STRING or null. ONE short sentence citing the concrete media/caption
  cue that supports each of is_sponsored, claimed_results, and the
  brand_safety_risk flags (say what you saw). null if none of those are set.

Qualitative categories (name the natural value; example possibilities are
GUIDANCE ONLY, never a constraint — if none fit, invent a precise new label):
- hook_type: e.g. question, bold claim, curiosity gap, pattern interrupt,
  personal story, contrarian take, shocking stat, direct value promise
- format_structure: e.g. tutorial, demo, case study, comparison, Q&A,
  behind-the-scenes, day-in-life, debunk/myth-bust, news/update, review,
  personal story, curated collection
- value_medium: e.g. live demonstration, talking-head explanation, screen
  recording, text-overlay slides, static illustration/photo, B-roll + voiceover
- text_overlay: e.g. none, burned-in captions, title-only, dense on-screen text
- cta_type: e.g. none, comment, save, share, follow, link-in-bio, DM, download

Count:
- hashtag_count: integer, number of hashtags in the caption (0 if none).
"""

_ENUM_DEFS = """\
Qualitative categories (pick the CLOSEST single value from the allowed set; if
genuinely none fit, use "other" and add a precise value after it as "other:<label>"):
- hook_type: one of {question, bold_claim, curiosity_gap, pattern_interrupt,
  personal_story, contrarian_take, shocking_stat, direct_value_promise, other}
- format_structure: one of {tutorial, demo, case_study, comparison, q_a,
  behind_the_scenes, day_in_life, debunk, news_update, review, personal_story,
  curated_collection, other}
- value_medium: one of {live_demonstration, talking_head, screen_recording,
  text_overlay_slides, static_illustration_photo, broll_voiceover, other}
- text_overlay: one of {none, burned_in_captions, title_only,
  dense_on_screen_text, other}
- cta_type: one of {none, comment, save, share, follow, link_in_bio, dm,
  download, other}
"""

_CODEBOOK_DENSE = """\
DECISION RULES (apply these; the examples are illustrative, not exhaustive):
- is_sponsored = TRUE only when the post is actively pushing a paid relationship
  or product for commercial benefit: an explicit "paid partnership"/#ad/gifted
  disclosure, an affiliate or discount code, or a product demo whose clear intent
  is selling that brand's item. FALSE when a product is merely mentioned
  neutrally, used personally with no code/ask, or is the creator's own offer.
  + e.g. "use code ALI10" or "#ad" -> TRUE. "I love this app I use daily" with no
  code or link -> FALSE.
- claimed_results: capture ONLY a stated specific outcome/result/guarantee
  (number reached, income, growth %, a before/after result, "guaranteed").
  + "grew to 300k in 90 days" -> capture. "121 days, thank you" (a milestone, no
  stated result) -> NULL. A price, follower count with no causal claim, or date ->
  NULL.
- hook_type names the opening *technique*, not the topic. "watch until the end
  for the one thing" = direct_value_promise/curiosity gap, not "tutorial".
- brand_safety_risk.profanity = TRUE only for strong/explicit profanity, not mild
  or censored slang. sensitive_adjacency = TRUE for unverified health/medical,
  guaranteed finance returns, or trauma-triggering content shown without care.
- value_depth: "deep" requires substantive explanation/teaching of a method or
  concept, not just a list of tips ("practical") or pure entertainment
  ("shallow").
"""


def build_prompt(variant: str) -> str:
    body = _OPEN_SCHEMA
    if variant == "V1":
        body += "\n" + _ENUM_DEFS
    elif variant == "V2":
        body += "\n" + _CODEBOOK_DENSE
    elif variant == "V3":
        body += "\n" + _ENUM_DEFS + "\n" + _CODEBOOK_DENSE  # enum categoricals + codebook rules
    return ("You are a meticulous content analyst. " + body + "\n" +
            "Caption:\n{caption}\n\n" + _RESERVED)


_FIELDS = [
    "face_present", "original_audio", "is_sponsored", "audience_named",
    "brand_safety_risk.profanity", "brand_safety_risk.sensitive_adjacency",
    "has_brand_tag", "value_depth", "brand_logos", "claimed_results",
    "sponsorship_signal", "hook_content", "replicable_tactic", "evidence",
    "hook_type", "format_structure", "value_medium", "text_overlay",
    "cta_type", "hashtag_count",
]


def _get(obj, dotted):
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


def resolve_all(posts, ops, gemini, include_media):
    resolved = {}
    live = []
    for p in posts:
        mf = resolve_post_media(p, ops, gemini) if include_media else []
        if include_media and not mf and p["media_files"]:
            continue
        resolved[p["post_id"]] = mf
        live.append(p)
    return live, resolved


def _poll(gemini, names, timeout_s=3000):
    from google.genai import Client
    client = Client(api_key=gemini.api_key)
    out = {}
    done = set()
    deadline = time.time() + timeout_s
    while len(done) < len(names):
        for i, name in enumerate(names):
            if i in done:
                continue
            job = client.batches.get(name=name)
            if gemini_batch.is_terminal(gemini_batch.job_state(job)):
                done.add(i)
                try:
                    out.update(gemini_batch.retrieve(gemini, name))
                except Exception as exc:  # noqa: BLE001
                    logger.error("retrieve %s: %s", name, exc)
        if len(done) < len(names):
            if time.time() > deadline:
                raise TimeoutError("batch not terminal")
            time.sleep(15)
    return out


def cmd_run(args):
    import duckdb
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set"); return 1
    from datalake.defs.instagram.config import GeminiTierConfig
    if not GeminiTierConfig.detect().supports_batch:
        print("need tier1+"); return 1

    variants = [v.strip().upper() for v in args.variants.split(",")]
    ops = SQLiteResource(database=os.path.join(REPO_ROOT, args.ops_db))
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])
    out = os.path.join(REPO_ROOT, args.out)
    state_db = os.path.join(REPO_ROOT, args.state_db)

    posts = sample_posts(state_db, args.sample, args.seed)
    live, resolved = resolve_all(posts, ops, gemini, args.media)
    if not live:
        print("all posts dropped"); return 1
    mtypes = {p["post_id"]: p["media_type"] for p in live}
    print(f"{len(live)} posts live: "
          f"{ {t: sum(1 for p in live if p['media_type']==t) for t in ('video','carousel','image')} }")

    con = duckdb.connect(out)
    con.execute("""CREATE TABLE IF NOT EXISTS facet_menu(
        post_id VARCHAR, variant VARCHAR, run INTEGER, media_type VARCHAR,
        result_json TEXT, ok BOOLEAN, error TEXT,
        PRIMARY KEY(post_id, variant, run))""")
    con.close()

    for variant in variants:
        prompt = build_prompt(variant)
        for run in range(args.runs):
            reqs = []
            for p in live:
                req = {"custom_key": f"{variant}:{run}:{p['post_id']}",
                       "prompt": prompt.replace("{caption}", p["caption"]),
                       "post_id": p["post_id"], "variant": variant, "run": run}
                if args.media and resolved[p["post_id"]]:
                    req["media_files"] = resolved[p["post_id"]]
                reqs.append(req)
            names = gemini_batch.submit(gemini, _DEFAULT_GEMINI_MODEL, reqs,
                                        f"facet-menu-{variant.lower()}-r{run}")
            print(f"{variant} r{run}: submitted {len(names)} job(s), {len(reqs)} reqs")
            results = _poll(gemini, names)
            ok = sum(1 for r in results.values() if r["ok"])
            print(f"{variant} r{run}: {ok}/{len(reqs)} ok")
            con = duckdb.connect(out)
            for key, rec in results.items():
                m = re.match(r"(V\d):(\d+):(.+)", key)
                if not m:
                    continue
                v, r, pid = m.group(1), int(m.group(2)), m.group(3)
                con.execute(
                    "INSERT OR REPLACE INTO facet_menu VALUES (?,?,?,?,?,?,?)",
                    [pid, v, r, mtypes.get(pid), rec.get("text"), rec.get("ok"),
                     rec.get("error")])
            con.close()
    print(f"done -> {out} ({len(live)} posts x {len(variants)} variants x {args.runs} runs)")
    return 0


def _ac1(vals_a, vals_b):
    """Gwet AC1 for two raters on equal-length nominal vectors (prevalence-robust)."""
    n = len(vals_a)
    if n == 0:
        return float("nan")
    agree = sum(1 for a, b in zip(vals_a, vals_b) if _norm(a) == _norm(b))
    pa = agree / n
    # observed category marginal probabilities (pooled)
    from collections import Counter
    c = Counter(_norm(x) for x in vals_a + vals_b)
    pe = sum((cnt / (2 * n)) ** 2 for cnt in c.values())
    return (pa - pe) / (1 - pe) if pe < 1 else float("nan")


def _pct(frac):
    return float("nan") if frac is None else 100.0 * frac


def cmd_analyze(args):
    import duckdb
    con = duckdb.connect(os.path.join(REPO_ROOT, args.out), read_only=True)
    rows = con.execute(
        "SELECT post_id, variant, run, media_type, result_json, ok "
        "FROM facet_menu").fetchall()
    con.close()
    if not rows:
        print("no results"); return 0
    # only pairwise-analyze run0 vs run1 agreement + collapse across all runs
    data = {}
    for pid, var, run, mt, rj, ok in rows:
        try:
            obj = json.loads(rj) if rj else {}
        except json.JSONDecodeError:
            obj = {}
        data.setdefault(var, {}).setdefault(pid, {})[run] = obj
    variants = sorted(data)
    print(f"variants: {variants}; posts: "
          f"{ {v: len(data[v]) for v in variants} }")
    for variant in variants:
        print(f"\n=== {variant} ===")
        bymedia = {}
        for pid, runs in data[variant].items():
            mt = next((r[3] for r in rows if r[0] == pid and r[1] == variant), "?")
            bymedia.setdefault(mt, []).append(runs)
        runsets = sorted({r for runs in data[variant].values() for r in runs})
        print(f"{'field':32} {'AC1(r0,r1)':>11} {'collapse%':>10} {'NA%':>6}")
        for field in _FIELDS:
            # collapse = fraction of single-bucket among run0 (most common bucket share)
            single = 0
            tot = 0
            na = 0
            for pid, runs in data[variant].items():
                v = _get(runs.get(0, {}), field)
                tot += 1
                if v is None:
                    na += 1
            # agreement only where run0 & run1 present together
            a = [_get(data[variant][pid].get(0, {}), field)
                 for pid in data[variant]]
            b = [_get(data[variant][pid].get(1, {}), field)
                 for pid in data[variant]]
            present = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
            ac1 = _ac1(*zip(*present)) if present else float("nan")
            # collapse over run0 present values: share of the modal bucket
            from collections import Counter
            vals = [_get(data[variant][pid].get(0, {}), field) for pid in data[variant]]
            vals = [v for v in vals if v is not None]
            c = Counter(_norm(v) for v in vals)
            coll = (max(c.values()) / len(vals)) if vals else 1.0
            print(f"{field:32} {ac1:11.2f} {100*coll:10.0f} {100*na/tot:6.0f}")
    return 0


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--sample", type=int, default=120)
    r.add_argument("--runs", type=int, default=2)
    r.add_argument("--variants", default="V0,V1,V2")
    r.add_argument("--seed", type=int, default=11)
    r.add_argument("--no-media", action="store_true")
    r.add_argument("--state-db", default="data/state.duckdb")
    r.add_argument("--ops-db", default="data/ops.sqlite")
    r.add_argument("--out", default=DEFAULT_OUT)
    a = sub.add_parser("analyze")
    a.add_argument("--out", default=DEFAULT_OUT)
    return p.parse_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _parse_args(argv)
    args.media = not getattr(args, "no_media", False)
    return cmd_run(args) if args.cmd == "run" else cmd_analyze(args)


if __name__ == "__main__":
    sys.exit(main())
