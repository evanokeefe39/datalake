#!/usr/bin/env python3
"""EDA: US-EFAC-2 — engagement-utility validation of V3 growth facets.

Reads `data/facet_menu.duckdb` (variant='V3', run 0, ok rows) and
`data/state.duckdb` (read-only) and answers one question per facet: does the
facet value separate standout/hot posts from underperformers?

Outcome semantics (canonical, held on the current label_version):
  high  = ig_post_labels.enrich_decision = 'standout' at MAX(label_version)
  low   = v_post_metrics.sigma_tier = '-1σ'          (underperformer)
  other = everything else (control/normal/skip) — kept for coverage stats,
          reported via mean likes_zscore only.

Media type is DERIVED (silver has no media_type column), following the
serving-layer convention (`v_engagement_outliers` treats video_view_count > 0
as the video gate):
  video    = video_view_count > 0
  carousel = media_count > 1
  image    = otherwise

Determinism: read-only connections, no RNG, no wall-clock in output, every
frame sorted by post_id / value before aggregation. Re-running against an
unchanged DB produces byte-identical files. The `--seed` flag is accepted for
harness parity with the other EDA scripts and is not used (no sampling occurs).

Usage:
  uv run python analysis/eda_facet_engagement.py [--seed 42]
      [--facet-menu data/facet_menu.duckdb] [--state data/state.duckdb]
      [--out analysis/output]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

DEFAULT_FACET_MENU = "data/facet_menu.duckdb"
DEFAULT_STATE = "data/state.duckdb"
DEFAULT_OUT = Path("analysis/output")

MIN_CELL = 10        # primary-table minimum n (high+low classed posts)
DECISION_CELL = 10   # minimum n for keep/refine/drop verdict
OVER_INDEX_BAR = 1.25
KEEP_BAR = 1.5

# Facet fields (design doc §4): the enum/categorical/bool V3 schema.
BOOL_FACETS = [
    "face_present", "audience_named", "is_sponsored", "has_brand_tag",
    "original_audio",
]
CAT_FACETS = [
    "value_depth", "hook_type", "format_structure", "value_medium",
    "text_overlay", "cta_type", "sponsorship_signal",
]
# brand_safety_risk dict → two bool facets (the explicit-set brand-safety model)
BRAND_SAFETY_KEYS = ["profanity", "sensitive_adjacency"]
# Free-text keys present in V3 JSON that are NOT facets (no enum, no gate):
FREE_TEXT_KEYS = ["hook_content", "claimed_results", "replicable_tactic", "evidence"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--seed", type=int, default=42,
                   help="accepted for determinism parity; unused (no sampling)")
    p.add_argument("--facet-menu", default=DEFAULT_FACET_MENU)
    p.add_argument("--state", default=DEFAULT_STATE)
    p.add_argument("--out", default=str(DEFAULT_OUT))
    return p.parse_args()


def tolerant_json(j: str) -> dict | None:
    """Parse model-emitted JSON; tolerate trailing commas, else None."""
    try:
        d = json.loads(j)
    except json.JSONDecodeError:
        try:
            d = json.loads(re.sub(r",\s*([}\]])", r"\1", j))
        except json.JSONDecodeError:
            return None
    return d if isinstance(d, dict) else None


def hashtag_bucket(n: int) -> str:
    if n == 0:
        return "0"
    if n <= 2:
        return "1-2"
    if n <= 4:
        return "3-4"
    return "5+"


def facet_rows(d: dict) -> dict[str, str]:
    """Flatten one post's V3 result_json into facet=value string pairs.

    Missing values become the literal "(missing)" so coverage is reported,
    never silently dropped (matches the eda_cta_education convention).
    """
    rows: dict[str, str] = {}
    for f in BOOL_FACETS:
        v = d.get(f)
        rows[f] = "true" if v is True else "false" if v is False else "(missing)"
    for f in CAT_FACETS:
        v = d.get(f)
        rows[f] = str(v) if v is not None else "(missing)"
    bs = d.get("brand_safety_risk") or {}
    for k in BRAND_SAFETY_KEYS:
        v = bs.get(k) if isinstance(bs, dict) else None
        rows[f"brand_safety_{k}"] = "true" if v is True else "false" if v is False else "(missing)"
    logos = d.get("brand_logos") or []
    rows["any_brand_logos"] = "true" if isinstance(logos, list) and logos else "false"
    hc = d.get("hashtag_count")
    rows["hashtag_count_bucket"] = hashtag_bucket(hc) if isinstance(hc, int) else "(missing)"
    return rows


def load_frames(facet_db: str, state_db: str) -> tuple[dict[str, dict], list[dict], dict]:
    fc = duckdb.connect(facet_db, read_only=True)
    # Deterministic order: post_id, then run — we take run 0 and keep run 1 only
    # to report the unparseable count (agreement already validated in design §4).
    raw = fc.sql(
        "select post_id, run, ok, result_json from facet_menu "
        "where variant = 'V3' order by post_id, run"
    ).fetchall()
    fc.close()

    parsed: dict[str, dict] = {}          # post_id -> facets at run 0
    run1_flips: Counter[str] = Counter()  # facet -> inter-run value flips
    unparseable = 0
    r0_total = r1_total = 0
    for pid, run, ok, js in raw:
        if run == 0:
            r0_total += 1
        else:
            r1_total += 1
        if not ok:
            continue
        d = tolerant_json(js)
        if d is None:
            if run == 0:
                unparseable += 1
            continue
        if run == 0:
            parsed[pid] = facet_rows(d)
        else:
            base = parsed.get(pid)
            if base is not None:
                other = facet_rows(d)
                for k in base:
                    if base[k] != other[k]:
                        run1_flips[k] += 1
    sc = duckdb.connect(state_db, read_only=True)
    lv = sc.sql("select max(label_version) from ig_post_labels").fetchone()[0]
    ids = ",".join(repr(p) for p in sorted(parsed))
    # silver has no media_type column — derived per the docstring convention.
    q = f"""
        select p.post_id,
               case when p.video_view_count > 0 then 'video'
                    when p.media_count > 1 then 'carousel'
                    else 'image' end as media_type,
               l.enrich_decision,
               coalesce(m.sigma_tier, 'none') as sigma_tier,
               m.likes_zscore
        from silver_ig_posts p
        left join v_post_metrics m using (post_id)
        left join ig_post_labels l
               on l.post_id = p.post_id and l.label_version = {lv}
        where p.post_id in ({ids})
        order by p.post_id
    """
    joined: list[dict] = []
    for pid, mt, dec, sig, lz in sc.sql(q).fetchall():
        outcome = ("high" if dec == "standout"
                   else "low" if sig == "-1σ" else "other")
        joined.append({
            "post_id": pid, "media_type": mt, "decision": dec or "(missing)",
            "sigma_tier": sig, "likes_zscore": lz, "outcome": outcome,
            **parsed[pid],
        })
    gold = sc.sql(
        f"select count(distinct post_id) posts, count(distinct gold_subtopic) subs "
        f"from v_post_detail where post_id in ({ids})"
    ).fetchone()
    sc.close()

    meta = {
        "label_version": lv, "unparseable_run0": unparseable,
        "v3_rows_run0": r0_total, "v3_rows_run1": r1_total,
        "gold_posts": gold[0], "gold_distinct_subtopics": gold[1],
        "run1_flips": run1_flips,
    }
    return parsed, joined, meta


ALL_FACETS = (
    BOOL_FACETS + CAT_FACETS
    + [f"brand_safety_{k}" for k in BRAND_SAFETY_KEYS]
    + ["any_brand_logos", "hashtag_count_bucket"]
)


def stats_for(rows: list[dict], facet: str) -> list[dict]:
    """Per-value stats for one facet: class counts, over-index, mean z."""
    glob_high = sum(1 for r in rows if r["outcome"] == "high")
    glob_low = sum(1 for r in rows if r["outcome"] == "low")
    n_classed = glob_high + glob_low
    ghs = glob_high / n_classed if n_classed else 0.0
    gls = glob_low / n_classed if n_classed else 0.0

    by_val: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_val[r[facet]].append(r)

    out = []
    for val, vr in sorted(by_val.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        n = len(vr)
        hi = sum(1 for r in vr if r["outcome"] == "high")
        lo = sum(1 for r in vr if r["outcome"] == "low")
        zs = [r["likes_zscore"] for r in vr if r["likes_zscore"] is not None]
        out.append({
            "facet": facet, "value": val, "n": n,
            "high_n": hi, "high_rate": hi / n if n else 0.0,
            "low_n": lo, "low_rate": lo / n if n else 0.0,
            "over_index": (hi / n) / ghs if n and ghs else None,
            "low_over_index": (lo / n) / gls if n and gls else None,
            "mean_z": sum(zs) / len(zs) if zs else None,
        })
    return out


def verdict(values: list[dict]) -> str:
    """keep / refine / drop for one facet over its per-value rows."""
    if len(values) <= 1:
        return "drop (zero-variance)"
    elig = [v for v in values if v["n"] >= DECISION_CELL]
    best_hi = max((v["over_index"] or 0 for v in elig), default=0.0)
    best_lo = max((v["low_over_index"] or 0 for v in elig), default=0.0)
    if best_hi >= KEEP_BAR or best_lo >= KEEP_BAR:
        return "keep"
    if best_hi >= OVER_INDEX_BAR or best_lo >= OVER_INDEX_BAR:
        return "refine"
    # no primary-cell signal: does a thin cell (n>=5) still point somewhere?
    thin_hi = max((v["over_index"] or 0 for v in values if v["n"] >= 5), default=0.0)
    thin_lo = max((v["low_over_index"] or 0 for v in values if v["n"] >= 5), default=0.0)
    if thin_hi >= KEEP_BAR or thin_lo >= KEEP_BAR:
        return "refine (thin-cell signal only)"
    return "drop (no discrimination)"


def md_table(values: list[dict], title: str, note: str | None = None) -> list[str]:
    lines = [f"### {title}", "",
             "| value | n | high | high_rate | low | low_rate | over_index | low_over_index | mean_z |",
             "|---|---|---|---|---|---|---|---|---|"]
    for v in values:
        oi = f"{v['over_index']:.2f}" if v["over_index"] is not None else "—"
        li = f"{v['low_over_index']:.2f}" if v["low_over_index"] is not None else "—"
        mz = f"{v['mean_z']:.2f}" if v["mean_z"] is not None else "—"
        flag = " *" if v["n"] < MIN_CELL else ""
        lines.append(
            f"| {v['value']}{flag} | {v['n']} | {v['high_n']} | "
            f"{v['high_rate']:.3f} | {v['low_n']} | {v['low_rate']:.3f} | "
            f"{oi} | {li} | {mz} |"
        )
    if note:
        lines += ["", note]
    return lines


def csv_rows(values: list[dict]) -> list[tuple]:
    return [
        (v["facet"], v["value"], v["n"], v["high_n"], round(v["high_rate"], 4),
         v["low_n"], round(v["low_rate"], 4),
         round(v["over_index"], 4) if v["over_index"] is not None else "",
         round(v["low_over_index"], 4) if v["low_over_index"] is not None else "",
         round(v["mean_z"], 4) if v["mean_z"] is not None else "")
        for v in values
    ]


CSV_HEADER = ["facet", "value", "n", "high_n", "high_rate", "low_n", "low_rate",
              "over_index", "low_over_index", "mean_z"]


def write_csv(path: Path, rows: list[tuple]) -> None:
    lines = [",".join(CSV_HEADER)]
    for r in rows:
        cells = []
        for c in r:
            s = str(c)
            cells.append(f'"{s}"' if ("," in s or '"' in s) else s)
        lines.append(",".join(cells))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    _, joined, meta = load_frames(args.facet_menu, args.state)
    n = len(joined)
    high = [r for r in joined if r["outcome"] == "high"]
    low = [r for r in joined if r["outcome"] == "low"]
    other = n - len(high) - len(low)

    mt_counts = Counter(r["media_type"] for r in joined)
    mt_outcome = Counter((r["media_type"], r["outcome"]) for r in joined)
    dec_counts = Counter(r["decision"] for r in joined)

    # ---- per-facet, pooled ----
    pooled = {f: stats_for(joined, f) for f in ALL_FACETS}
    verdicts = {f: verdict(pooled[f]) for f in ALL_FACETS}

    # zero-variance facets
    zero_var = [f for f in ALL_FACETS
                if len({r[f] for r in joined if r[f] != "(missing)"}) <= 1]

    # ---- per-media-type ----
    per_mt: dict[str, dict[str, list[dict]]] = {}
    for mt in ("video", "carousel", "image"):
        rows = [r for r in joined if r["media_type"] == mt]
        per_mt[mt] = {f: stats_for(rows, f) for f in ALL_FACETS}

    # ---- report ----
    L: list[str] = []
    L.append("# US-EFAC-2 — Engagement-utility validation of V3 growth facets")
    L.append("")
    L.append("Does each V3 facet value separate standout/hot posts from")
    L.append("underperformers? Grounded in the 95-post `facet_menu` V3 run joined to")
    L.append("lake engagement state. Reproduce with")
    L.append("`uv run python analysis/eda_facet_engagement.py`.")
    L.append("")
    L.append("## Method")
    L.append("")
    L.append(f"- Source: `data/facet_menu.duckdb`, `variant='V3'`, **run 0**,")
    L.append(f"  `{meta['v3_rows_run0']}` rows, all `ok`; run 1 (`{meta['v3_rows_run1']}` rows)")
    L.append("  is used only to count inter-run value flips (agreement itself was")
    L.append("  validated in the design doc §4, so it is not re-measured here).")
    L.append(f"- Outcome (`label_version={meta['label_version']}`): **high** =")
    L.append("  `ig_post_labels.enrich_decision='standout'`; **low** =")
    L.append("  `v_post_metrics.sigma_tier='-1σ'`; everything else is classed `other`")
    L.append("  (kept for coverage, not for the discrimination rates).")
    L.append("- Media type is **derived** (silver has no `media_type` column):")
    L.append("  `video_view_count>0` → video; `media_count>1` → carousel; else image.")
    L.append("- `over_index` = value's high-rate ÷ global high-rate over classed")
    L.append(f"  posts (n≥{MIN_CELL} cells are the primary signal; smaller cells are")
    L.append("  marked `*` and never drive a verdict).")
    L.append("- Verdict rule: **keep** = a value with n≥10 at over_index ≥ 1.5")
    L.append("  (either direction); **refine** = signal between 1.25 and 1.5, or all")
    L.append("  cells n<10; **drop** = zero variance or max over_index < 1.25.")
    L.append("")
    L.append("## Coverage (honesty first)")
    L.append("")
    L.append("| segment | n | high | low | other |")
    L.append("|---|---|---|---|---|")
    L.append(f"| pooled | {n} | {len(high)} | {len(low)} | {other} |")
    for mt in ("video", "carousel", "image"):
        hi = mt_outcome.get((mt, "high"), 0)
        lo = mt_outcome.get((mt, "low"), 0)
        L.append(f"| {mt} | {mt_counts[mt]} | {hi} | {lo} | {mt_counts[mt]-hi-lo} |")
    L.append("")
    L.append(f"- Label decisions on the facet posts: "
             + ", ".join(f"`{k}`={v}" for k, v in sorted(dec_counts.items()))
             + ".")
    L.append(f"- Unparseable V3 result_json rows (run 0): {meta['unparseable_run0']}.")
    L.append(f"- Niche split: gold covers {meta['gold_posts']}/95 facet posts but")
    L.append(f"  yields {meta['gold_distinct_subtopics']} distinct subtopics (~1 post")
    L.append("  per niche) — **a per-niche facet table would be all thin cells**, so")
    L.append("  it is not computed. Niche-level facet utility needs a larger corpus")
    L.append("  or coarser niche grouping (e.g. `gold_topic`, still ~1/subtopic).")
    L.append("- Classed posts per media type: video 5 high / 19 low, carousel")
    L.append("  2 high / 6 low, image 1 high / 1 low — per-media rates are")
    L.append("  indicative only; nothing per-media-type clears a keep-verdict bar.")
    L.append("")
    L.append("## Per-facet discrimination (pooled)")
    L.append("")
    for f in ALL_FACETS:
        note = None
        if f in zero_var:
            note = "Zero-variance facet — flagged for removal (design §4 rule)."
        L += md_table(pooled[f], f"`{f}` — {verdicts[f]}", note)
        L.append("")
    L.append("*(n < 10 marked `*`.)*")
    L.append("")
    L.append("## Inter-run stability of each facet (run 0 vs run 1)")
    L.append("")
    L.append("| facet | flipped posts / 94 |")
    L.append("|---|---|")
    for f in ALL_FACETS:
        L.append(f"| {f} | {meta['run1_flips'].get(f, 0)} |")
    L.append("")
    L.append("## Per-media-type tables")
    L.append("")
    L.append("Only the facets with any classable signal per media type are shown;")
    L.append("full CSVs carry every facet × value row.")
    for mt in ("video", "carousel", "image"):
        L.append(f"### {mt} (n={mt_counts[mt]})")
        L.append("")
        shown = False
        for f in ALL_FACETS:
            vals = per_mt[mt][f]
            if len(vals) <= 1 or len({v['value'] for v in vals}) == 1:
                continue
            interesting = [v for v in vals
                           if v["high_n"] + v["low_n"] > 0 and v["n"] >= 5]
            if not interesting:
                continue
            L += md_table(vals, f"{mt} — `{f}`")
            L.append("")
            shown = True
        if not shown:
            L.append("_No facet had a classable cell (n≥5 with high/low posts) on")
            L.append("this media type — consistent with 2–10 classed posts total._")
            L.append("")
    L.append("## Free-text keys (not facets)")
    L.append("")
    L.append("V3 JSON also carries `hook_content`, `claimed_results`,")
    L.append("`replicable_tactic`, `evidence` — free text with no enum, so they")
    L.append("cannot be validated by this screen and are excluded. They serve")
    L.append("summaries/search, not decision-gated facets (design §6).")
    L.append("")
    L.append("## Provenance & caveats")
    L.append("")
    L.append(f"- Script: `analysis/eda_facet_engagement.py` (deterministic,")
    L.append("  read-only `duckdb.connect(..., read_only=True)`; `--seed` accepted")
    L.append("  but unused — no sampling).")
    L.append("- Data: `data/facet_menu.duckdb` V3 run 0/1 (95 posts);")
    L.append(f"  `data/state.duckdb` `silver_ig_posts`, `ig_post_labels`")
    L.append(f"  (label_version={meta['label_version']}), `v_post_metrics`,")
    L.append("  `v_post_detail`. No writes to any database.")
    L.append("- n=95 with 9 high / 26 low classed posts is a thin base: pooled")
    L.append("  cells with n<10 are indicative, not conclusive. Verdicts marked")
    L.append("  'refine (thin cells)' should be re-tested after the facet pass")
    L.append("  ships lake-wide.")
    L.append("- Standout labels are label-pass output on the current label")
    L.append("  version, not human gold; re-materializing the lake can legitimately")
    L.append("  change these numbers.")
    L.append("")
    (out / "eda_facet_engagement.md").write_text("\n".join(L) + "\n", encoding="utf-8")

    write_csv(out / "eda_facet_engagement.csv", csv_rows_flat(pooled))
    for mt in ("video", "carousel", "image"):
        write_csv(out / f"eda_facet_engagement__{mt}.csv",
                  csv_rows_flat(per_mt[mt]))
    return 0


def csv_rows_flat(per_facet: dict[str, list[dict]]) -> list[tuple]:
    rows: list[tuple] = []
    for f in ALL_FACETS:
        rows += csv_rows(per_facet[f])
    return rows


if __name__ == "__main__":
    raise SystemExit(main())
