#!/usr/bin/env python3
"""EDA: content-quality axis — comparative over-index tables.

For each content axis, compares each value's standout rate and underperformer
rate against the GLOBAL rates, producing a decision-ready over-index table:

    value | n_total | standout_n | standout_rate | underperf_n | underperf_rate
    | over_index (standout_rate / global_standout_rate)
    | underperf_over_index (underperf_rate / global_underperf_rate)

Global rates are COMPUTED from the same frame (never hardcoded):
    global_standout_rate   = standout posts / labeled posts
    global_underperf_rate  = underperformer posts / labeled posts

Tables:
  - PRIMARY per-value table: values with n_total >= MIN_CELL (10), sorted by
    over_index DESC. Compact (~tens of rows), thin cells (n < 5) flagged.
  - LONG TAIL: top 15 values with 5 <= n_total < MIN_CELL by over_index, so
    emerging labels are visible without the n=1 dump.
  - DECISION ROWS per axis:
      * best-replicate candidates: n >= 10, over_index >= 1.25 AND
        underperf_over_index <= 1.0 (over-indexes standout without
        over-indexing underperformance), best first.
      * avoid candidates: n >= 10, underperf_over_index >= 1.25, worst first.

Reads data/state.duckdb READ-ONLY; label_version pinned to MAX in
ig_post_labels. Deterministic: fixed ORDER BY, no timestamps/RNG.
Writes markdown + per-axis CSVs under analysis/output/.

Usage:
    uv run python analysis/eda_content_axis.py [--db PATH] [--out DIR]
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

AXES = [
    "gold_topic",
    "gold_subtopic",
    "gold_domain",
    "gold_subdomain",
    "content_type",
    "format",
    "style",
    "admiralty",
]
FLAGS = ["is_educational", "is_actionable", "has_engagement_bait"]

NEGATIVE_TIERS = ("-1σ", "-2σ", "-3σ")
MIN_CELL = 10       # primary-table minimum n_total
LONG_TAIL_N = 15    # long-tail section cap
THIN_CELL = 5
DECISION_CELL = 10  # minimum n for decision rows
OVER_INDEX_BAR = 1.25

# Deterministic full query: one row per labeled post at the current
# label_version, with segment + content attributes. ORDER BY post_id for a
# stable frame across runs.
BASE_SQL = """
WITH current_version AS (
    SELECT MAX(label_version) AS label_version FROM ig_post_labels
),
labeled AS (
    SELECT
        d.post_id,
        d.owner_id,
        d.owner_username,
        d.timestamp,
        d.likes_count,
        d.has_engagement_bait,
        d.gold_topic,
        d.gold_subtopic,
        d.gold_domain,
        d.gold_subdomain,
        d.content_type,
        d.format,
        d.style,
        d.admiralty,
        d.is_educational,
        d.is_actionable,
        l.label,
        l.is_provisional,
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
        description="Content-axis comparative over-index EDA."
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


def over_index_rows(df: pl.DataFrame, col: str) -> list[dict]:
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
            (pl.col("standout_n") / pl.col("n_total")).alias("standout_rate"),
            (pl.col("underperf_n") / pl.col("n_total")).alias("underperf_rate"),
        )
        .with_columns(
            pl.when(global_standout_rate > 0)
            .then(pl.col("standout_rate") / global_standout_rate)
            .otherwise(None)
            .alias("over_index"),
            pl.when(global_underperf_rate > 0)
            .then(pl.col("underperf_rate") / global_underperf_rate)
            .otherwise(None)
            .alias("underperf_over_index"),
        )
        .sort(
            ["over_index", "n_total", "value"],
            descending=[True, True, False],
            nulls_last=True,
        )
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
        for r in agg.iter_rows(named=True)
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


def decision_lines(df: pl.DataFrame, col: str, rows: list[dict]) -> list[str]:
    """Best-replicate / avoid candidate rows for one axis."""
    eligible = [r for r in rows if r["n_total"] >= DECISION_CELL]
    best = [
        r
        for r in eligible
        if (r["over_index"] or 0) >= OVER_INDEX_BAR
        and (r["underperf_over_index"] or 9.99) <= 1.0
    ]
    best.sort(key=lambda r: (-(r["over_index"] or 0), r["value"]))
    avoid = [r for r in eligible if (r["underperf_over_index"] or 0) >= OVER_INDEX_BAR]
    avoid.sort(key=lambda r: (-(r["underperf_over_index"] or 0), r["value"]))

    lines: list[str] = ["#### Decision rows", ""]
    lines.append("**Best-replicate candidates** "
                 f"(n≥{DECISION_CELL}, over_index≥{OVER_INDEX_BAR}, underperf_over_index≤1.0):")
    lines.append("")
    if best:
        for r in best[:10]:
            lines.append(
                f"- `{r['value']}` — over_index **{r['over_index']:.2f}**, "
                f"underperf_over_index {r['underperf_over_index']:.2f}, "
                f"n={r['n_total']} (standout {r['standout_n']}, underperf {r['underperf_n']})"
            )
    else:
        lines.append("- none meet all bars")
    lines.append("")
    lines.append("**Avoid candidates** "
                 f"(n≥{DECISION_CELL}, underperf_over_index≥{OVER_INDEX_BAR}):")
    lines.append("")
    if avoid:
        for r in avoid[:10]:
            lines.append(
                f"- `{r['value']}` — underperf_over_index **{r['underperf_over_index']:.2f}**, "
                f"over_index {r['over_index']:.2f}, "
                f"n={r['n_total']} (standout {r['standout_n']}, underperf {r['underperf_n']})"
            )
    else:
        lines.append("- none meet all bars")
    lines.append("")
    return lines


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
        df = pl.from_arrow(con.execute(BASE_SQL).arrow())
    finally:
        con.close()

    df = segment_frame(df)
    n_total = df.height
    seg_counts = {
        s: df.filter(pl.col("segment") == s).height
        for s in ("standout", "underperformer", "average")
    }
    n_provisional = df.filter(pl.col("is_provisional")).height
    global_standout_rate = seg_counts["standout"] / n_total if n_total else 0.0
    global_underperf_rate = seg_counts["underperformer"] / n_total if n_total else 0.0

    lines: list[str] = [
        "# Content-axis EDA — comparative over-index table",
        "",
        f"- label_version held constant: **{version}**",
        f"- posts analyzed: {n_total} "
        f"(standout {seg_counts['standout']}, underperformer {seg_counts['underperformer']}, "
        f"average {seg_counts['average']})",
        f"- **global_standout_rate = {seg_counts['standout']}/{n_total} = "
        f"{global_standout_rate:.1%}**; **global_underperf_rate = "
        f"{seg_counts['underperformer']}/{n_total} = {global_underperf_rate:.1%}** "
        "(computed from this frame — over_index 1.0 = average post)",
        "- `over_index` = standout_rate / global_standout_rate; "
        "`underperf_over_index` = underperf_rate / global_underperf_rate.",
        f"- Primary table: values with n_total ≥ {MIN_CELL}. Long tail: top "
        f"{LONG_TAIL_N} values with {THIN_CELL} ≤ n_total < {MIN_CELL}. "
        "Thin cells (n < "
        f"{THIN_CELL}) flagged ⚠thin.",
        "- label maturity: "
        f"{n_total - n_provisional} day-7 judgments, {n_provisional} provisional "
        "(included; less-trusted subset).",
        "- `format`/`style`/`content_type` labels derive from TEXT-ONLY signals "
        "(caption/hashtags/metadata), not visual inspection — treat as provisional.",
        "",
        "## Reading guide",
        "",
        "**Replicate**: high over_index AND underperf_over_index ≤ 1.0 "
        "(over-indexes standout without over-indexing underperformance). "
        "**Avoid**: high underperf_over_index. Both must clear n≥"
        f"{DECISION_CELL} to be actionable. See Decision rows per axis.",
        "",
    ]

    for axis in AXES + FLAGS:
        rows = over_index_rows(df, axis)
        write_csv(
            out_dir / f"content_axis__{axis}.csv",
            [
                (
                    r["value"],
                    r["n_total"],
                    r["standout_n"],
                    round(r["standout_rate"], 6),
                    r["underperf_n"],
                    round(r["underperf_rate"], 6),
                    round(r["over_index"], 4) if r["over_index"] is not None else "",
                    round(r["underperf_over_index"], 4) if r["underperf_over_index"] is not None else "",  # noqa: E501
                )
                for r in rows
            ],
            [
                "value", "n_total", "standout_n", "standout_rate",
                "underperf_n", "underperf_rate", "over_index",
                "underperf_over_index",
            ],
        )

        lines.append(f"## {axis}")
        lines.append("")

        primary = [r for r in rows if r["n_total"] >= MIN_CELL]
        lines += md_table(
            primary,
            f"Primary values (n_total ≥ {MIN_CELL}, sorted by over_index desc)",
            note=f"{len(primary)} values shown; {len(rows) - len(primary)} smaller values in long tail below." if rows else "no data",  # noqa: E501
        )
        lines += decision_lines(df, axis, rows)

        long_tail = [r for r in rows if THIN_CELL <= r["n_total"] < MIN_CELL]
        if long_tail:
            lines += md_table(
                long_tail[:LONG_TAIL_N],
                f"Long tail (top {LONG_TAIL_N} by over_index, {THIN_CELL} ≤ n < {MIN_CELL})",
            )
        lines.append("")

    md_path = out_dir / "content_axis.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"label_version={version} posts={n_total} segments={seg_counts}")
    print(f"global_standout_rate={global_standout_rate:.4f} "
          f"global_underperf_rate={global_underperf_rate:.4f}")
    print(f"wrote {md_path} and per-axis CSVs under {out_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
