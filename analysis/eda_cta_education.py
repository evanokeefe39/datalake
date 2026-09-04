#!/usr/bin/env python3
"""EDA: CTA / engagement bait / educational framing (research question Q6).

Outcome (standout / underperformer rates + over-index vs GLOBAL) by:
  - is_educational / is_actionable  (gold enrichment on v_post_detail)
  - has_engagement_bait             (silver_ig_posts signal on v_post_detail)
  - content_type                    (educational-vs-entertainment split)

CONFIRMED columns on v_post_detail (checked against the live DB):
  is_educational BOOLEAN, is_actionable BOOLEAN, has_engagement_bait BOOLEAN,
  content_type VARCHAR.
Data gap: the lake has NO dedicated CTA-type column (e.g. "comment-bait",
"save-CTA", "link-in-bio") — has_engagement_bait is the only bait signal, and
it is a boolean from the scraper, not a gold label. Nothing fabricated here;
that granularity is a named gap.

Reads data/state.duckdb READ-ONLY; label_version pinned to MAX in
ig_post_labels. Deterministic: fixed ORDER BY, no timestamps/RNG.
Writes markdown + CSVs under analysis/output/.

Usage:
    uv run python analysis/eda_cta_education.py [--db PATH] [--out DIR]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import polars as pl

DEFAULT_DB = "data/state.duckdb"
DEFAULT_OUT = Path("analysis/output")

NEGATIVE_TIERS = ("-1σ", "-2σ", "-3σ")
THIN_CELL = 5
MIN_CELL = 10        # primary-table minimum n_total
DECISION_CELL = 10   # minimum n for decision rows
OVER_INDEX_BAR = 1.25

FLAGS = ["is_educational", "is_actionable", "has_engagement_bait"]

# One row per labeled post at the current label_version with Q6 attributes.
BASE_SQL = """
WITH current_version AS (
    SELECT MAX(label_version) AS label_version FROM ig_post_labels
),
labeled AS (
    SELECT
        d.post_id,
        d.owner_id,
        d.owner_username,
        d.is_educational,
        d.is_actionable,
        d.has_engagement_bait,
        d.content_type,
        l.label_version,
        m.is_standout,
        m.sigma_tier
    FROM v_post_detail d
    JOIN ig_post_labels l ON d.post_id = l.post_id
    JOIN v_post_metrics m ON d.post_id = m.post_id
    CROSS JOIN current_version cv
    WHERE l.label_version = cv.label_version
)
SELECT * FROM labeled ORDER BY post_id
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CTA / educational-framing comparative over-index EDA (Q6)."
    )
    p.add_argument(
        "--db",
        default=os.environ.get("DATALAKE_DUCKDB", DEFAULT_DB),
        help="Path to the DuckDB database (default: data/state.duckdb or $DATALAKE_DUCKDB).",
    )
    p.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help="Output directory (default: analysis/output).",
    )
    return p.parse_args()


def write_csv(path: Path, rows: list[tuple], header: list[str]) -> None:
    pl.DataFrame(rows, schema=header, orient="row").write_csv(path)


def segment_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("sigma_tier").is_in(list(NEGATIVE_TIERS)))
        .then(pl.lit("underperformer"))
        .when(pl.col("is_standout") == 1)
        .then(pl.lit("standout"))
        .otherwise(pl.lit("average"))
        .alias("segment")
    )


def over_index_rows(df: pl.DataFrame, col: str, order: list[str] | None = None) -> list[dict]:
    """Per-value comparative stats for one axis. Deterministic sort inside."""
    n_all = df.height
    if n_all == 0:
        return []
    global_standout_rate = df.filter(pl.col("segment") == "standout").height / n_all
    global_underperf_rate = (
        df.filter(pl.col("segment") == "underperformer").height / n_all
    )
    agg = (
        df.with_columns(pl.col(col).fill_null("(missing)").cast(pl.Utf8).alias("value"))
        .group_by("value")
        .agg(
            pl.len().alias("n_total"),
            (pl.col("segment") == "standout").sum().alias("standout_n"),
            (pl.col("segment") == "underperformer").sum().alias("underperf_n"),
        )
        .with_columns(
            (pl.col("standout_n") / pl.col("n_total")).round(6).alias("standout_rate"),
            (pl.col("underperf_n") / pl.col("n_total")).round(6).alias("underperf_rate"),
        )
        .with_columns(
            pl.when(global_standout_rate > 0)
            .then((pl.col("standout_rate") / global_standout_rate).round(6))
            .otherwise(None)
            .alias("over_index"),
            pl.when(global_underperf_rate > 0)
            .then((pl.col("underperf_rate") / global_underperf_rate).round(6))
            .otherwise(None)
            .alias("underperf_over_index"),
        )
    )
    if order is not None:
        rank = {v: i for i, v in enumerate(order)}
        agg = agg.with_columns(
            pl.col("value")
            .map_elements(lambda v: rank.get(v, len(order)), return_dtype=pl.Int64)
            .alias("_ord")
        ).sort(["_ord", "value"])
    else:
        agg = agg.sort(
            ["over_index", "n_total", "value"],
            descending=[True, True, False],
            nulls_last=True,
        )
    rows = [
        {
            "value": r["value"],
            "n_total": int(r["n_total"]),
            "standout_n": int(r["standout_n"]),
            "standout_rate": float(r["standout_rate"]),
            "underperf_n": int(r["underperf_n"]),
            "underperf_rate": float(r["underperf_rate"]),
            "over_index": float(r["over_index"]) if r["over_index"] is not None else None,
            "underperf_over_index": (
                float(r["underperf_over_index"]) if r["underperf_over_index"] is not None else None
            ),
        }
        for r in agg.drop("_ord", strict=False).iter_rows(named=True)
    ]
    return rows


def md_table(rows: list[dict], title: str, note: str | None = None) -> list[str]:
    lines = [f"### {title}", "", "| value | n_total | standout_n | standout_rate | underperf_n | underperf_rate | over_index | underperf_over_index |", "|---|---|---|---|---|---|---|---|"]  # noqa: E501
    if note:
        lines += [note, ""]
    for r in rows:
        thin = " ⚠thin" if r["n_total"] < THIN_CELL else ""
        lines.append(
            f"| {r['value']}{thin} | {r['n_total']} | {r['standout_n']} "
            f"| {r['standout_rate']:.1%} | {r['underperf_n']} "
            f"| {r['underperf_rate']:.1%} "
            f"| {r['over_index']:.2f} | {r['underperf_over_index']:.2f} |"
        )
    lines.append("")
    return lines


def bool_flag_frame(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col)
        .map_elements(
            lambda b: {True: "true", False: "false", None: "(missing)"}.get(b, "(missing)"),
            return_dtype=pl.Utf8,
        )
        .alias(col)
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 1

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        version = con.execute(
            "SELECT MAX(label_version) FROM ig_post_labels"
        ).fetchone()[0]
        # Coverage check for the three flags (honest gaps, not fabrication).
        coverage = con.execute(
            """
            SELECT
                COUNT(*) AS n,
                COUNT(is_educational) AS n_edu,
                COUNT(is_actionable) AS n_act,
                COUNT(has_engagement_bait) AS n_bait,
                COUNT(content_type) AS n_ct
            FROM v_post_detail
            """
        ).fetchone()
        df = pl.from_arrow(con.execute(BASE_SQL).arrow())
    finally:
        con.close()

    if df.height == 0:
        print("No labeled posts found; nothing to report.", file=sys.stderr)
        return 1
    df = segment_frame(df)

    n_all, n_edu, n_act, n_bait, n_ct = coverage
    lines: list[str] = [
        "# EDA: CTA / engagement bait / educational framing (Q6)",
        "",
        f"label_version pinned: **{version}** — frame n = {df.height} labeled posts.",
        "",
        "Global rates computed from the same frame (never hardcoded).",
        "",
        "## Column coverage (v_post_detail, all posts — not just the labeled frame)",
        "",
        f"- `is_educational`: {n_edu}/{n_all} populated "
        f"({n_edu / n_all:.1%}) — gold enrichment; NULL means the post was never",
        "  enriched or the classifier returned NULL. See the `(missing)` rows:",
        "  they over-index standout in this corpus, so labelled-only comparisons",
        "  are the honest ones and `(missing)` is shown, not dropped.",
        f"- `is_actionable`: {n_act}/{n_all} populated ({n_act / n_all:.1%}).",
        f"- `has_engagement_bait`: {n_bait}/{n_all} populated "
        f"({n_bait / n_all:.1%}) — silver_ig_posts scraper signal (boolean).",
        f"- `content_type`: {n_ct}/{n_all} populated ({n_ct / n_all:.1%}) — gold.",
        "",
        "## DATA GAP (honest note)",
        "",
        "- No CTA-type label exists in the lake (no \"comment-bait\", \"save-CTA\",",
        "  \"follow-CTA\", \"link-in-bio\" taxonomy). `has_engagement_bait` is the",
        "  only bait signal and is scraper-derived (not gold-judged). Q6's CTA",
        "  granularity is therefore only answerable at true/false today; a",
        "  gold-CTA taxonomy would be a new label_version (web/enrichment work).",
        "",
    ]

    flag_tables: dict[str, list[dict]] = {}
    for flag in [*FLAGS, "content_type"]:
        flag_df = bool_flag_frame(df, flag) if flag in FLAGS else df
        order = ["true", "false", "(missing)"] if flag != "content_type" else None
        rows = over_index_rows(flag_df, flag, order=order)
        flag_tables[flag] = rows
        title = {
            "is_educational": "By is_educational (gold)",
            "is_actionable": "By is_actionable (gold)",
            "has_engagement_bait": "By has_engagement_bait (silver_ig_posts)",
            "content_type": "By content_type (gold) — educational vs entertainment",
        }[flag]
        note = (
            "Note: gold labels are free-text from one prompt generation; spelling"
            " variants exist (e.g. `curated_collection` vs `curated collection`)"
            " and are kept distinct rather than silently merged."
            if flag == "content_type" else None
        )
        lines += md_table(rows, title, note=note)
        if flag == "has_engagement_bait" and not any(
            r["value"] == "true" for r in rows
        ):
            lines += [
                "DATA GAP: `has_engagement_bait` has NO true values in this",
                "corpus — the flag exists but never fires, so the bait axis is",
                "degenerate (reported, not fabricated).",
                "",
            ]

    # Decision rows across all flags/content_type.
    eligible = [
        r for rows in flag_tables.values() for r in rows
        if r["n_total"] >= DECISION_CELL and r["value"] != "(missing)"
    ]
    best = [
        r for r in eligible
        if (r["over_index"] or 0) >= OVER_INDEX_BAR
        and (r["underperf_over_index"] or 9.99) <= 1.0
    ]
    best.sort(key=lambda r: (-(r["over_index"] or 0), r["value"]))
    avoid = [
        r for r in eligible if (r["underperf_over_index"] or 0) >= OVER_INDEX_BAR
    ]
    avoid.sort(key=lambda r: (-(r["underperf_over_index"] or 0), r["value"]))
    lines += [
        "## Decision rows (n≥10, over_index≥1.25 / underperf_over_index≤1.0; `(missing)` excluded)",
        "",
        "**Best-replicate candidates:**",
        "",
    ]
    lines += [
        f"- `{r['value']}` — over_index **{r['over_index']:.2f}**, "
        f"underperf_over_index {r['underperf_over_index']:.2f}, "
        f"n={r['n_total']} (standout {r['standout_n']}, underperf {r['underperf_n']})"
        for r in best[:10]
    ] or ["- none meet all bars"]
    lines += ["", "**Avoid candidates:**", ""]
    lines += [
        f"- `{r['value']}` — underperf_over_index **{r['underperf_over_index']:.2f}**, "
        f"over_index {r['over_index']:.2f}, "
        f"n={r['n_total']} (standout {r['standout_n']}, underperf {r['underperf_n']})"
        for r in avoid[:10]
    ] or ["- none meet all bars"]
    lines += [
        "",
        "Correlations, not causal effects; `true`/`false` labels come from one",
        "prompt generation and re-run after any label prompt change.",
        "",
    ]

    out_md = out_dir / "eda_cta_education.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")

    csv_header = [
        "value", "n_total", "standout_n", "standout_rate", "underperf_n",
        "underperf_rate", "over_index", "underperf_over_index",
    ]
    for flag, rows in flag_tables.items():
        write_csv(out_dir / f"eda_cta_education_{flag}.csv",
                  [tuple(r.values()) for r in rows], csv_header)

    print(f"Wrote {out_md} (+{len(flag_tables)} CSVs) — {df.height} posts, label_version {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
