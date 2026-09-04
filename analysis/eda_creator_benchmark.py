#!/usr/bin/env python3
"""EDA: creator benchmark — who to imitate (research question Q7).

Per-creator standout rate (v_creator_outlier_rate) and underperformer rate
(v_creator_underperformer_rate), with over-index vs population and thin-cell
flags on low post counts. Joins creator_id / creator_name for readability.

    creator | n_posts | standout_n | standout_rate | underperf_n | underperf_rate
    | over_index (standout_rate / global_standout_rate)
    | underperf_over_index (underperf_rate / global_underperf_rate)

NOTE (honest): the rollup views pool across label versions; per-version
recomputation is what analysis/eda_content_axis.py does. Global rates are
computed from the views' own totals so over-index is internally consistent.

Reads data/state.duckdb READ-ONLY. Deterministic: fixed ORDER BY, no
timestamps/RNG. Writes markdown + CSVs under analysis/output/.

Usage:
    uv run python analysis/eda_creator_benchmark.py [--db PATH] [--out DIR]
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

MIN_CELL = 10        # primary-table minimum n_posts
THIN_CELL = 5
DECISION_CELL = 10   # imitation-shortlist minimum n_posts
OVER_INDEX_BAR = 1.25

# v_creator_* rollups: per-creator totals + segment counts (pooled versions).
CREATOR_SQL = """
SELECT
    o.owner_username,
    o.creator_id,
    CAST(o.total_posts AS BIGINT) AS total_posts,
    CAST(o.outlier_posts AS BIGINT) AS standout_n,
    CAST(COALESCE(u.underperformer_posts, 0) AS BIGINT) AS underperf_n
FROM v_creator_outlier_rate o
LEFT JOIN v_creator_underperformer_rate u
    ON o.owner_id = u.owner_id
ORDER BY o.owner_username
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Per-creator standout/underperformer benchmark EDA (Q7)."
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


def creator_rows(df: pl.DataFrame, total_posts_all: int) -> list[dict]:
    """Per-creator comparative stats vs population. Deterministic sort inside."""
    if df.height == 0 or total_posts_all == 0:
        return []
    global_standout_rate = (
        df.select(pl.col("standout_n").sum()).item() / total_posts_all
    )
    global_underperf_rate = (
        df.select(pl.col("underperf_n").sum()).item() / total_posts_all
    )
    agg = (
        df.with_columns(
            (pl.col("standout_n") / pl.col("total_posts")).round(6).alias("standout_rate"),
            (pl.col("underperf_n") / pl.col("total_posts")).round(6).alias("underperf_rate"),
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
        .sort(
            ["over_index", "total_posts", "owner_username"],
            descending=[True, True, False],
            nulls_last=True,
        )
    )
    return [
        {
            "creator": r["owner_username"],
            "creator_id": r["creator_id"],
            "n_posts": int(r["total_posts"]),
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


def md_table(rows: list[dict], title: str, note: str | None = None) -> list[str]:
    lines = [f"### {title}", "", "| creator | creator_id | n_posts | standout_n | standout_rate | underperf_n | underperf_rate | over_index | underperf_over_index |", "|---|---|---|---|---|---|---|---|---|"]  # noqa: E501
    if note:
        lines += [note, ""]
    for r in rows:
        thin = " ⚠thin" if r["n_posts"] < THIN_CELL else ""
        lines.append(
            f"| {r['creator']}{thin} | {r['creator_id'] if r['creator_id'] is not None else ''} "
            f"| {r['n_posts']} | {r['standout_n']} | {r['standout_rate']:.1%} "
            f"| {r['underperf_n']} | {r['underperf_rate']:.1%} "
            f"| {r['over_index']:.2f} | {r['underperf_over_index']:.2f} |"
        )
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
        df = pl.from_arrow(con.execute(CREATOR_SQL).arrow())
        total_posts_all = con.execute(
            "SELECT COUNT(*) FROM v_post_detail"
        ).fetchone()[0]
    finally:
        con.close()

    if df.height == 0:
        print("No creator rollup rows found; nothing to report.", file=sys.stderr)
        return 1
    rows = creator_rows(df, total_posts_all)

    n_creators = len(rows)
    thin = sum(1 for r in rows if r["n_posts"] < THIN_CELL)
    lines: list[str] = [
        "# EDA: Creator benchmark — who to imitate (Q7)",
        "",
        f"Frame: {n_creators} creators from v_creator_outlier_rate ⨝ "
        "v_creator_underperformer_rate (pooled across label versions).",
        "",
        f"Population baseline: {total_posts_all} posts in v_post_detail.",
        "Global rates computed from the views' own totals (never hardcoded).",
        "",
        "## Verdict and caveats",
        "",
        "- Rollup views pool across label versions — internal validity is lower",
        "  than the per-version recomputation in analysis/eda_content_axis.py.",
        "- Rates are UNADJUSTED for follower tier: a creator with 100k followers",
        "  has more standout posts for mechanical reasons, not skill. Tier",
        "  context per analysis/eda_follower_tier.py before imitating.",
        f"- {thin}/{n_creators} creators have n_posts < {THIN_CELL} (⚠thin);",
        f" primary table keeps n_posts ≥ {MIN_CELL}, long tail caps at top 15.",
        "- Survivorship: every creator here was worth scraping. This is a",
        "  shortlist of who to imitate WITHIN the niche, not a market census.",
        "",
    ]

    primary = [r for r in rows if r["n_posts"] >= MIN_CELL]
    tail = [
        r for r in rows
        if THIN_CELL <= r["n_posts"] < MIN_CELL
    ][:15]
    lines += md_table(
        primary,
        f"Primary table (n_posts ≥ {MIN_CELL}, sorted by over_index DESC)",
    )
    lines += md_table(
        tail,
        f"Long tail (top 15 by over_index, {THIN_CELL} ≤ n_posts < {MIN_CELL})",
    )

    # Imitation shortlist + explicit avoid list.
    eligible = [r for r in rows if r["n_posts"] >= DECISION_CELL]
    best = [
        r for r in eligible
        if (r["over_index"] or 0) >= OVER_INDEX_BAR
        and (r["underperf_over_index"] or 9.99) <= 1.0
    ]
    best.sort(key=lambda r: (-(r["over_index"] or 0), r["creator"]))
    avoid = [
        r for r in eligible if (r["underperf_over_index"] or 0) >= OVER_INDEX_BAR
    ]
    avoid.sort(key=lambda r: (-(r["underperf_over_index"] or 0), r["creator"]))
    lines += [
        f"## Decision rows (n≥{DECISION_CELL}, over_index≥{OVER_INDEX_BAR} / underperf_over_index≤1.0)",  # noqa: E501
        "",
        "**Imitation shortlist:**",
        "",
    ]
    lines += [
        f"- @{r['creator']} (creator_id {r['creator_id']}) — over_index "
        f"**{r['over_index']:.2f}**, underperf_over_index {r['underperf_over_index']:.2f}, "
        f"n={r['n_posts']} (standout {r['standout_n']}, underperf {r['underperf_n']})"
        for r in best[:10]
    ] or ["- none meet all bars"]
    lines += [
        "",
        "**Underperformance-prone (study what NOT to do):**",
        "",
    ]
    lines += [
        f"- @{r['creator']} — underperf_over_index **{r['underperf_over_index']:.2f}**, "
        f"over_index {r['over_index']:.2f}, n={r['n_posts']}"
        for r in avoid[:10]
    ] or ["- none meet all bars"]
    lines.append("")

    out_md = out_dir / "eda_creator_benchmark.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")

    csv_header = [
        "creator", "creator_id", "n_posts", "standout_n", "standout_rate",
        "underperf_n", "underperf_rate", "over_index", "underperf_over_index",
    ]
    write_csv(out_dir / "eda_creator_benchmark.csv",
              [tuple(r.values()) for r in rows], csv_header)

    print(f"Wrote {out_md} (+1 CSV) — {n_creators} creators, population {total_posts_all}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
