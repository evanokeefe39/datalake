#!/usr/bin/env python3
"""EDA: posting cadence / timing (research question Q5) — descriptive only.

Compares standout / underperformer rates and over-index vs the GLOBAL rates
for each posting day-of-week and hour-of-day (local timestamp from
v_post_detail; dim_date.day_of_week / is_weekend join via v_post_detail.dim_date
when present).

    value | n_total | standout_n | standout_rate | underperf_n | underperf_rate
    | over_index (standout_rate / global_standout_rate)
    | underperf_over_index (underperf_rate / global_underperf_rate)

Reads data/state.duckdb READ-ONLY; label_version pinned to MAX in
ig_post_labels. Deterministic: fixed ORDER BY, no timestamps/RNG.
Writes markdown + CSVs under analysis/output/.

Usage:
    uv run python analysis/eda_cadence_timing.py [--db PATH] [--out DIR]
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
OVER_INDEX_BAR = 1.25

DAY_ORDER = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
]

# One row per labeled post at the current label_version with timing fields.
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
        dd.day_of_week,
        dd.is_weekend,
        extract(hour FROM d.timestamp)::INTEGER AS hour_of_day,
        l.label_version,
        m.is_standout,
        m.sigma_tier
    FROM v_post_detail d
    JOIN ig_post_labels l ON d.post_id = l.post_id
    JOIN v_post_metrics m ON d.post_id = m.post_id
    LEFT JOIN dim_date dd ON d.dim_date = dd.date
    CROSS JOIN current_version cv
    WHERE l.label_version = cv.label_version
)
SELECT * FROM labeled ORDER BY post_id
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cadence/timing comparative over-index EDA (Q5)."
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
    """Per-value comparative stats for one timing axis. Deterministic sort inside."""
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
        # Fixed calendar/clock order; unknown values appended alphabetically.
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

    if df.height == 0:
        print("No labeled posts found; nothing to report.", file=sys.stderr)
        return 1
    df = segment_frame(df)

    lines: list[str] = [
        "# EDA: Posting cadence / timing (Q5)",
        "",
        f"label_version pinned: **{version}** — frame n = {df.height} labeled posts.",
        "",
        "Global rates computed from the same frame (never hardcoded).",
        "",
        "## Verdict and caveats",
        "",
        "- **Descriptive only.** No experiment, so this is correlation over an",
        "  observational corpus with heavy confounding by creator activity level",
        "  (active creators post at many hours AND have more standout posts).",
        "- Timestamps are Instagram post timestamps as scraped; timezone is the",
        "  scraper's capture convention, not the audience's local clock — treat",
        "  hour-of-day as a relative prior, not an absolute schedule.",
        "- Cells with n < 5 are flagged ⚠thin, never silently aggregated.",
        "",
    ]

    day_rows = over_index_rows(df, "day_of_week", order=DAY_ORDER)
    missing_days = df.filter(pl.col("day_of_week").is_null()).height
    lines += md_table(
        day_rows,
        "By day of week (dim_date.day_of_week via v_post_detail.dim_date)",
        note=(
            "Calendar order; over_index sorts are secondary. "
            f"Posts without a dim_date join: {missing_days} "
            f"({missing_days / df.height:.1%})."
        ),
    )

    hour_rows = over_index_rows(df, "hour_of_day")
    lines += md_table(hour_rows, "By hour of day (extract(hour FROM timestamp))")

    weekend_rows = over_index_rows(df, "is_weekend")
    lines += md_table(weekend_rows, "Weekend vs weekday (dim_date.is_weekend)")

    # Decision rows: schedule priors only, honest bars.
    eligible = [r for r in day_rows + hour_rows if r["n_total"] >= MIN_CELL]
    best = [
        r for r in eligible
        if (r["over_index"] or 0) >= OVER_INDEX_BAR
        and (r["underperf_over_index"] or 9.99) <= 1.0
    ]
    avoid = [
        r for r in eligible
        if (r["underperf_over_index"] or 0) >= OVER_INDEX_BAR
    ]
    best.sort(key=lambda r: (-(r["over_index"] or 0), r["value"]))
    avoid.sort(key=lambda r: (-(r["underperf_over_index"] or 0), r["value"]))
    lines += [
        "## Decision rows (schedule priors, n≥10, over_index≥1.25 / underperf_over_index≤1.0)",
        "",
        "**Over-indexing standout slots:**",
        "",
    ]
    lines += [
        f"- `{r['value']}` — over_index **{r['over_index']:.2f}**, "
        f"underperf_over_index {r['underperf_over_index']:.2f}, n={r['n_total']}"
        for r in best[:10]
    ] or ["- none meet all bars"]
    lines += ["", "**Over-indexing underperformance slots:**", ""]
    lines += [
        f"- `{r['value']}` — underperf_over_index **{r['underperf_over_index']:.2f}**, "
        f"over_index {r['over_index']:.2f}, n={r['n_total']}"
        for r in avoid[:10]
    ] or ["- none meet all bars"]
    lines += [
        "",
        "These are priors for a posting schedule, not causal effects.",
        "",
    ]

    out_md = out_dir / "eda_cadence_timing.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")

    csv_header = [
        "value", "n_total", "standout_n", "standout_rate", "underperf_n",
        "underperf_rate", "over_index", "underperf_over_index",
    ]
    write_csv(out_dir / "eda_cadence_timing_day_of_week.csv",
              [tuple(r.values()) for r in day_rows], csv_header)
    write_csv(out_dir / "eda_cadence_timing_hour_of_day.csv",
              [tuple(r.values()) for r in hour_rows], csv_header)
    write_csv(out_dir / "eda_cadence_timing_weekend.csv",
              [tuple(r.values()) for r in weekend_rows], csv_header)

    print(f"Wrote {out_md} (+3 CSVs) — {df.height} posts, label_version {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
