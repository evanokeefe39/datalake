"""Spike: fold facets+summary into ONE media call vs TWO calls (facets | summary).

Question: does asking a single Gemini media call for BOTH visual facets AND a
content/per-image summary degrade either output (task interference / output-token
pressure) enough to justify paying a SECOND video-input call? Cost is the crux:
Mode B re-sends the video (reusing the same uploaded File-API URI), so B pays
~2x the video input of A.

Design (no human gold — the user rejected manual labeling):
  * Facet quality proxy: field-by-field agreement between A.visual_facets and
    B_F.visual_facets (per-field Gwet AC1 / equality). Low agreement under fold
    => folding perturbs facets => prefer two calls.
  * Summary quality proxy: (a) richness (char/token length), (b) a paired,
    label-randomized TEXT judge comparing A's summary vs B_S's summary per post
    (better / more informative / more grounded, using the caption as context).
  * Validity: JSON parse rate, missing required fields, truncation, and carousel
    image_summaries length == number of images (index-alignment).
  * Cost: count video-input calls per mode (A=1/post, B=2/post) from estimates.

Writes ONLY to a temp DB (default data/facet_summary_spike.duckdb). Reuses defs
helpers but needs a local submitter to set max_output_tokens per mode (the batch
path hardcodes 2048). Runs in scripts/ => does not trip media_upload seam guards.
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
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL  # noqa: E402
from scripts.facet_experiment import sample_posts, resolve_post_media  # noqa: E402

logger = logging.getLogger("facet_summary_spike")

DEFAULT_OUT = "data/facet_summary_spike.duckdb"

# ── Shared task bodies (visual-necessary core, codebook-lite) ────────────────
_FACET_KEYS = (
    "face_present (bool), is_sponsored (bool), sponsorship_signal (string, empty if "
    "none), brand_logos (array of strings, empty if none), value_medium (one of "
    "demo|talking_head|screenshare|broll_voiceover|slideshow_carousel|text_graphic|"
    "unclear), text_overlay_present (bool), on_screen_claim (bool), evidence "
    "(string, one short sentence citing what you actually saw)"
)
_FACET_RULES = (
    "face_present=true ONLY if a human face is visible. is_sponsored=true ONLY if "
    "you SEE a sponsorship signal (prominent brand product, logo, tag, or a pushed "
    "code/offer) rather than an ordinary mention. value_medium = how the content is "
    "primarily delivered. text_overlay_present=true only if text is rendered ON the "
    "image/video. on_screen_claim=true only if text ON screen asserts a claim (a "
    "number, result, or promise) -- not mere speech. brand_logos = visible brand "
    "names/marks."
)
_SUMMARY_RULE = "Describe only what is visible; never infer the unseen."

def _facet_task() -> str:
    return ("TASK A -- VISUAL FACETS. Output these keys: " + _FACET_KEYS + ".\n"
            "Rules: " + _FACET_RULES)

def _summary_task(n_media: int) -> str:
    if n_media > 1:
        return ("TASK B -- PER-IMAGE SUMMARY. This is a multi-image carousel; output "
                "image_summaries as an array with EXACTLY " + str(n_media) + " "
                "entries, one per image in display order; each "
                "{index:int, summary:string}, one short sentence. " + _SUMMARY_RULE)
    return ("TASK B -- VISUAL SUMMARY. Output content_summary: string (2-3 short "
            "sentences) describing what this media SHOWS: scene, subject(s), actions, "
            "on-screen text, visual narrative. " + _SUMMARY_RULE)

def _intro(caption: str) -> str:
    return ("You are a meticulous content analyst. Judge the media for the tasks "
            "below. Caption (context only, NOT grounds for visual claims):\n"
            + caption + "\n\n")

def _facet_shape() -> str:
    return ("Return ONE JSON object with EXACTLY these keys: face_present, "
            "is_sponsored, sponsorship_signal, brand_logos, value_medium, "
            "text_overlay_present, on_screen_claim, evidence.")

def _mk_prompt(caption: str, n_media: int, summary: bool, facets: bool) -> str:
    """B_F = facets only; B_S = summary only. n_media>1 => carousel per-image."""
    head = _intro(caption)
    if facets and not summary:
        return head + _facet_shape() + "\n" + _facet_task()
    if summary and not facets:
        if n_media > 1:
            return (head + "Return ONE JSON object with EXACTLY these keys: "
                    "image_summaries (array).\n" + _summary_task(n_media))
        return (head + "Return ONE JSON object with EXACTLY these keys: "
                "content_summary (string).\n" + _summary_task(n_media))
    return head + _summary_task(n_media)

def prompt_mode_a(caption: str, n_media: int) -> str:
    """One call returns BOTH visual_facets and the summary in a single JSON object."""
    head = _intro(caption)
    if n_media > 1:
        shape = ("Return ONE JSON object with EXACTLY these two keys: "
                 "visual_facets (object, TASK A keys) and image_summaries (array).")
    else:
        shape = ("Return ONE JSON object with EXACTLY these two keys: "
                 "visual_facets (object, TASK A keys) and content_summary (string).")
    return head + shape + "\n" + _facet_task() + "\n" + _summary_task(n_media)

# ── Local submitter with per-mode max_output_tokens (defs batch path is 2048-only) ──
def _to_inlined(prompt: str, media_files: list | None, out_tokens: int,
                custom_key: str):
    from google.genai.types import InlinedRequest, GenerateContentConfig
    media = bool(media_files)
    body = gemini_batch._build_contents(prompt, media_files) if media else None
    kwargs: dict = dict(response_mime_type="application/json",
                        temperature=0.2, max_output_tokens=out_tokens)
    if media:
        kwargs["media_resolution"] = "MEDIA_RESOLUTION_LOW"
    return InlinedRequest(
        contents=(body if body is not None else prompt),
        config=GenerateContentConfig(**kwargs),
        metadata={"custom_key": custom_key},
    )

def _submit(gemini: GeminiResource, requests: list[dict], name: str,
            out_tokens: int) -> list[str]:
    """Chunk + create batch jobs (mirrors gemini_batch.submit but honors out_tokens)."""
    client = gemini_batch._client(gemini)
    from datalake.defs.instagram.config import GeminiTierConfig
    chunks = gemini_batch.chunk_requests(
        requests, GeminiTierConfig.detect().max_batch_tokens)
    names: list[str] = []
    for i, chunk in enumerate(chunks):
        inlined = [
            _to_inlined(req["prompt"], req.get("media_files"), out_tokens,
                        req["custom_key"])
            for req in chunk
        ]
        display = name if len(chunks) == 1 else f"{name}-seg{i}"
        job = client.batches.create(model=_DEFAULT_GEMINI_MODEL, src=inlined,
                                    config={"display_name": display})
        if not job.name:
            raise RuntimeError(f"no name: {display}")
        names.append(job.name)
    return names

def _poll(gemini: GeminiResource, names: list[str], timeout_s: int = 3600) -> dict:
    client = gemini_batch._client(gemini)
    out: dict = {}
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

# ── Parsing helpers ───────────────────────────────────────────────────────────
def _parse(text: str | None) -> dict:
    if not text:
        return {}
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}

def cmd_run(args):
    import duckdb
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set"); return 1
    from datalake.defs.instagram.config import GeminiTierConfig
    if not GeminiTierConfig.detect().supports_batch:
        print("need tier1+"); return 1

    ops = SQLiteResource(database=os.path.join(REPO_ROOT, args.ops_db))
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])
    out = os.path.join(REPO_ROOT, args.out)
    state_db = os.path.join(REPO_ROOT, args.state_db)

    # sample a pool large enough to survive the dead-media drop, then resolve once
    posts = sample_posts(state_db, args.sample, args.seed,
                         targets={"video": args.video, "carousel": args.carousel,
                                  "image": 0})
    live = []
    resolved = {}
    for p in posts:
        mf = resolve_post_media(p, ops, gemini)
        if not mf:
            continue
        live.append(p)
        resolved[p["post_id"]] = mf

    # quota-select per type so --video/--carousel are the ACTUAL live counts used
    quotas = {"video": args.video, "carousel": args.carousel}
    counts: dict = {"video": 0, "carousel": 0}
    kept = []
    for p in live:
        t = p["media_type"]
        if t in quotas and counts[t] < quotas[t]:
            counts[t] += 1
            kept.append(p)
    live = kept
    if len(live) < 4:
        print(f"too few after quota ({len(live)}); raise --sample / lower targets"); return 1
    print(f"using {len(live)}: "
          f"{ {t: sum(1 for p in live if p['media_type']==t) for t in ('video','carousel')} }")

    con = duckdb.connect(out)
    con.execute("""CREATE TABLE IF NOT EXISTS facet_summary_spike(
        post_id VARCHAR, mode VARCHAR, result_json TEXT, ok BOOLEAN, error TEXT,
        n_media INTEGER, PRIMARY KEY(post_id, mode))""")
    con.close()

    # Build all requests per mode
    # A : facets+summary one call   (out 4096)
    # B_F: facets only               (out 2048)
    # B_S: summary only              (out 4096)
    want = [m.strip().upper() for m in args.modes.split(",")]
    modes = {"A": 4096, "B_F": 2048, "B_S": 4096}
    grouped = {m: [] for m in modes}
    for p in live:
        pid = p["post_id"]
        n = len(resolved[pid])
        cap = p["caption"] or ""
        if "A" in want:
            grouped["A"].append({"custom_key": f"A::{pid}", "post_id": pid,
                                 "prompt": prompt_mode_a(cap, n),
                                 "media_files": resolved[pid]})
        if "B_F" in want:
            grouped["B_F"].append({"custom_key": f"B_F::{pid}", "post_id": pid,
                                   "prompt": _mk_prompt(cap, n, summary=False, facets=True),
                                   "media_files": resolved[pid]})
        if "B_S" in want:
            grouped["B_S"].append({"custom_key": f"B_S::{pid}", "post_id": pid,
                                   "prompt": _mk_prompt(cap, n, summary=True, facets=False),
                                   "media_files": resolved[pid]})

    for mode in want:
        if not grouped[mode]:
            continue
        out_tokens = modes[mode]
        reqs = grouped[mode]
        names = _submit(gemini, reqs, f"fss-{mode.lower()}", out_tokens)
        print(f"{mode}: submitted {len(names)} job(s), {len(reqs)} reqs "
              f"(out<=>{out_tokens})")
        results = _poll(gemini, names)
        ok = sum(1 for r in results.values() if r["ok"])
        print(f"{mode}: {ok}/{len(reqs)} ok")
        con = duckdb.connect(out)
        for key, rec in results.items():
            pid = key.split("::", 1)[1]
            con.execute(
                "INSERT OR REPLACE INTO facet_summary_spike VALUES (?,?,?,?,?,?)",
                [pid, mode, rec.get("text"), rec.get("ok"), rec.get("error"),
                 len(resolved.get(pid, []))])
        con.close()
    print(f"done -> {out} ({len(live)} posts x {want})")
    return 0

def cmd_judge(args):
    """Paired, label-randomized text judge: A summary vs B_S summary per post."""
    import duckdb
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set"); return 1
    from datalake.defs.instagram.config import GeminiTierConfig
    if not GeminiTierConfig.detect().supports_batch:
        print("need tier1+"); return 1
    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])
    out = os.path.join(REPO_ROOT, args.out)
    state_db = os.path.join(REPO_ROOT, args.state_db)
    con = duckdb.connect(out, read_only=True)
    rows = con.execute(
        "SELECT post_id, mode, result_json, ok FROM facet_summary_spike").fetchall()
    con.close()
    # caption map from silver
    import duckdb as _db
    scon = _db.connect(state_db, read_only=True)
    caps = {r[0]: (r[1] or "") for r in scon.execute(
        "SELECT post_id, caption FROM silver_ig_posts").fetchall()}
    scon.close()
    # gather per-post A & B_S summaries
    sumA, sumB = {}, {}
    for pid, mode, rj, ok in rows:
        if mode not in ("A", "B_S") or not ok:
            continue
        obj = _parse(rj)
        s = None
        if "image_summaries" in obj and isinstance(obj["image_summaries"], list):
            s = " | ".join(x.get("summary", "") for x in obj["image_summaries"]
                           if isinstance(x, dict))
        elif obj.get("content_summary"):
            s = obj["content_summary"]
        if s:
            (sumA if mode == "A" else sumB)[pid] = s
    common = [p for p in sumA if p in sumB]
    if not common:
        print("no paired A/B_S summaries to judge"); return 0
    random.seed(args.seed)
    reqs = []
    mapping = {}
    for pid in common:
        flip = random.random() < 0.5  # blind which is A vs B_S
        (s1, s2) = (sumA[pid], sumB[pid]) if not flip else (sumB[pid], sumA[pid])
        mapping[f"J::{pid}"] = {"a_is_s1": not flip, "post_id": pid}
        reqs.append({
            "custom_key": f"J::{pid}",
            "prompt": _JUDGE_PROMPT.replace("{caption}", caps.get(pid, ""))
                                    .replace("{summary1}", s1)
                                    .replace("{summary2}", s2),
        })
    names = _submit(gemini, reqs, "fss-judge", 512)
    results = _poll(gemini, names)
    con = duckdb.connect(out)
    con.execute("""CREATE TABLE IF NOT EXISTS summary_judge(
        post_id VARCHAR, a_is_s1 BOOLEAN, better VARCHAR, more_informative VARCHAR,
        more_grounded VARCHAR, PRIMARY KEY(post_id))""")
    for key, rec in results.items():
        m = mapping.get(key)
        if not m or not rec["ok"]:
            continue
        obj = _parse(rec["text"])
        # interpret in terms of A
        def _map(field):
            v = obj.get(field)
            if v not in ("summary1", "summary2"):
                return "tie"
            chosen_is_s1 = v == "summary1"
            # does 'chosen' correspond to A?
            return "A" if chosen_is_s1 == m["a_is_s1"] else "B_S"
        con.execute(
            "INSERT OR REPLACE INTO summary_judge VALUES (?,?,?,?,?)",
            [m["post_id"], m["a_is_s1"], _map("better"),
             _map("more_informative"), _map("more_grounded")])
    con.close()
    print(f"judged {len(reqs)} paired summaries")
    return 0

_JUDGE_PROMPT = (
    "Two different model summaries describe the SAME visual media. The caption is:\n"
    "{caption}\n\nSummary 1:\n{summary1}\n\nSummary 2:\n{summary2}\n\n"
    'Which is better overall? Consider how specific, informative, and faithful '
    "to what is likely SHOWN (given the caption) each is. Return ONLY a JSON object "
    'like {"better":"summary1|summary2|tie","more_informative":"summary1|summary2|tie",'
    '"more_grounded":"summary1|summary2|tie"}. Prefer ties only when indistinguishable.'
)

def cmd_analyze(args):
    import duckdb
    con = duckdb.connect(os.path.join(REPO_ROOT, args.out), read_only=True)
    rows = con.execute(
        "SELECT post_id, mode, result_json, ok, n_media FROM facet_summary_spike").fetchall()
    con.close()
    if not rows:
        print("no results"); return 0

    # ---- field-level facet agreement: A.visual_facets vs B_F.visual_facets
    facets = ["face_present", "is_sponsored", "sponsorship_signal", "brand_logos",
              "value_medium", "text_overlay_present", "on_screen_claim"]
    aobj, bf = {}, {}
    for pid, mode, rj, ok, n in rows:
        if not ok:
            continue
        o = _parse(rj)
        if mode == "A":
            vf = o.get("visual_facets") if isinstance(o, dict) else None
            if isinstance(vf, dict):
                aobj[pid] = (vf, n)
        elif mode == "B_F":
            # B_F returns bare facet keys at top level (not nested under visual_facets)
            if isinstance(o, dict) and "face_present" in o:
                bf[pid] = o
    common = [p for p in aobj if p in bf]
    print(f"\nfacet agreement (A-folded vs B-facets-only), n={len(common)}")
    print(f"{'field':24}{'agree%':>8}{'NA':>5}")
    from collections import Counter
    for f in facets:
        pairs = []
        for p in common:
            av = aobj[p][0].get(f)
            bv = bf[p].get(f)
            if av is None or bv is None:
                continue
            pairs.append((av, bv))
        if not pairs:
            print(f"{f:24}{'-':>8}{'-':>5}"); continue
        agree = sum(1 for a, b in pairs if json.dumps(a, sort_keys=True) ==
                    json.dumps(b, sort_keys=True))
        print(f"{f:24}{100*agree/len(pairs):7.0f}%{len(pairs):>5}")

    # ---- summary richness (A vs B_S)
    def _summary(obj):
        if isinstance(obj, dict) and "image_summaries" in obj and isinstance(obj["image_summaries"], list):
            return " ".join(x.get("summary", "") for x in obj["image_summaries"] if isinstance(x, dict))
        if isinstance(obj, dict):
            return obj.get("content_summary", "")
        return ""
    la, lb = {}, {}
    for pid, mode, rj, ok, n in rows:
        if not ok:
            continue
        s = _summary(_parse(rj))
        if mode == "A":
            la[pid] = (s, n)
        elif mode == "B_S":
            lb[pid] = s
    common2 = [p for p in la if p in lb]
    lens_a = [len(la[p][0]) for p in common2]
    lens_b = [len(lb[p]) for p in common2]
    if common2:
        print(f"\nsummary richness (chars): A mean={sum(lens_a)/len(lens_a):.0f} "
              f"B_S mean={sum(lens_b)/len(lens_b):.0f}")

    # ---- carousel index alignment
    mis = tot = 0
    for pid, mode, rj, ok, n in rows:
        if mode != "A" or not ok or n <= 1:
            continue
        o = _parse(rj)
        arr = o.get("image_summaries") if isinstance(o, dict) else None
        tot += 1
        if not isinstance(arr, list) or len(arr) != n:
            mis += 1
    if tot:
        print(f"carousel index-alignment (A): {tot - mis}/{tot} matched n_media")

    # ---- cost estimate (input-dominated): A=1 media call/post, B=2 media calls/post
    n_media_posts = len(rows) // 3 if rows else 0
    print(f"\ncost: A = 1 video-input call/post ({n_media_posts} calls for "
          f"{n_media_posts} posts); B = 2 video-input calls/post ({2*n_media_posts}) "
          "-- B pays ~2x A's video input (plus 3x caption text, negligible)")
    return 0

def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--sample", type=int, default=80, help="pool to sample from")
    r.add_argument("--video", type=int, default=10)
    r.add_argument("--carousel", type=int, default=10)
    r.add_argument("--modes", default="A,B_F,B_S",
                   help="comma list of modes to run (resume support)")
    r.add_argument("--seed", type=int, default=21)
    r.add_argument("--state-db", default="data/state.duckdb")
    r.add_argument("--ops-db", default="data/ops.sqlite")
    r.add_argument("--out", default=DEFAULT_OUT)
    j = sub.add_parser("judge")
    j.add_argument("--state-db", default="data/state.duckdb")
    j.add_argument("--seed", type=int, default=7)
    j.add_argument("--out", default=DEFAULT_OUT)
    a = sub.add_parser("analyze")
    a.add_argument("--out", default=DEFAULT_OUT)
    return p.parse_args(argv)

def main(argv=None):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    args = _parse_args(argv)
    return {"run": cmd_run, "judge": cmd_judge, "analyze": cmd_analyze}[args.cmd](args)

if __name__ == "__main__":
    sys.exit(main())
