#!/usr/bin/env python3
"""EDA: follower-tier stratification of the content-quality axis.

Stratifies the standout / underperformer / average segments by the OWNER'S
FOLLOWER TIER at (approximately) post time, computed DIRECTLY from
silver_ig_profile_observations — independent of any serving-layer attribution
view:

  - Join each post to the owner's follower observation NEAREST AT-OR-AFTER
    the post timestamp (earliest observation with observed_at >= post
    timestamp); ties prefer the observation whose source_dataset matches the
    post's, then the lexicographically smallest owner_id.
  - owner_id match first; owners with no observations fall back to matching
    on owner_username.
  - Tiers: 0-100 / 100-1k / 1k-10k / 10k-100k / 100k+ (followers_count
    NULL -> no tier; a genuine 0 counts as 0-100).

OUTPUT SHAPE — comparative, not a count dump:
  - Per axis x tier: a COMPARATIVE over-index table
      value | n_total | standout_n | standout_rate | underperf_n
      | underperf_rate | over_index (standout_rate / GLOBAL_standout_rate)
      | underperf_over_index (underperf_rate / GLOBAL_underperf_rate)
    where the GLOBAL rates are computed from the same attributed frame.
    Primary table: n_total >= MIN_CELL (10), sorted by over_index DESC.
    Decision rows (best-replicate / avoid) per axis x tier.
  - Tier-level over-index table: is the TIER itself over- or under-indexing
    standout/underperformance vs the whole labeled population?

HONEST CAVEATS (also printed in the output):
  - Follower GROWTH over time is NOT yet observable. The bronze backfill
    yields ~58 observations over ~50 owners across 6 files — most owners
    have a SINGLE observation — so tier is a snapshot attribute, not a
    trajectory. The 0→100→1k→10k mechanics remain web-first until forward
    scrapes accumulate a real series.
  - Because the nearest observation may be AFTER the post, tier is
    "follower level around the post", not a strictly at-post-time measure.

Reads data/state.duckdb (or $DATALAKE_DUCKDB) READ-ONLY; writes deterministic
CSV + markdown under analysis/output/. Flags thin cells (n < 5) rather than
hiding them.

Usage:
    uv run python analysis/eda_follower_tier.py [--db PATH] [--out DIR]
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
MIN_CELL = 10        # primary comparative-table minimum n_total
LONG_TAIL_N = 10     # long-tail section cap per (axis, tier)
DECISION_CELL = 10
OVER_INDEX_BAR = 1.25
TIERS = ["0-100", "100-1k", "1k-10k", "10k-100k", "100k+", "unknown"]

TIERS_SQL = """
CASE
    WHEN followers_at_post IS NULL THEN 'unknown'
    WHEN followers_at_post < 100   THEN '0-100'
    WHEN followers_at_post < 1000  THEN '100-1k'
    WHEN followers_at_post < 100000 THEN '10k-100k'
    ELSE '100k+'
END
"""


# Post -> nearest at-or-after follower observation. owner_id join first;
# owner_username fallback only for owners absent from the observation table
# by owner_id.
ATTRIBUTION_SQL = f"""
WITH current_version AS (
    SELECT MAX(label_version) AS label_version FROM ig_post_labels
),
posts AS (
    SELECT
        d.post_id, d.owner_id, d.owner_username, d.timestamp, d.source_dataset,
        d.gold_topic, d.gold_subtopic, d.gold_domain, d.gold_subdomain,
        d.content_type, d.format, d.style, d.admiralty,
        d.is_educational, d.is_actionable, d.has_engagement_bait,
        m.is_standout, m.sigma_tier,
        l.label_version
    FROM v_post_detail d
    JOIN ig_post_labels l ON d.post_id = l.post_id
    JOIN v_post_metrics m ON d.post_id = m.post_id
    CROSS JOIN current_version cv
    WHERE l.label_version = cv.label_version
),
obs_by_id AS (
    -- nearest observation at-or-after the post ts, per owner_id
    SELECT DISTINCT ON (p.post_id)
        p.post_id,
        o.followers_count,
        o.observed_at,
        p.source_dataset,
        'owner_id' AS match_key
    FROM posts p
    JOIN silver_ig_profile_observations o ON o.owner_id = p.owner_id
    WHERE o.observed_at >= p.timestamp
    ORDER BY p.post_id,
             o.observed_at ASC,
             CASE WHEN o.source_dataset = p.source_dataset THEN 0 ELSE 1 END,
             o.owner_id
),
obs_by_username AS (
    -- fallback for posts whose owner_id has NO observations at all
    SELECT DISTINCT ON (p.post_id)
        p.post_id,
        o.followers_count,
        o.observed_at,
        o.source_dataset AS obs_source_dataset,
        'owner_username' AS match_key
    FROM posts p
    JOIN silver_ig_profile_observations o
        ON lower(o.owner_username) = lower(p.owner_username)
    WHERE o.observed_at >= p.timestamp
      AND p.post_id NOT IN (SELECT post_id FROM obs_by_id)
    ORDER BY p.post_id,
             o.observed_at ASC,
             CASE WHEN o.source_dataset = p.source_dataset THEN 0 ELSE 1 END,
             o.owner_id
),
attributed AS (
    SELECT
        p.*,
        COALESCE(i.followers_count, u.followers_count) AS followers_at_post,
        COALESCE(i.observed_at, u.observed_at) AS attribution_observed_at,
        COALESCE(i.match_key, u.match_key) AS match_key,
        {TIERS_SQL} AS follower_tier
    FROM posts p
    LEFT JOIN obs_by_id i ON p.post_id = i.post_id
    LEFT JOIN obs_by_username u ON p.post_id = u.post_id
)
SELECT * FROM attributed ORDER BY post_id
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Follower-tier stratification of the content-quality axis "
        "(comparative over-index tables)."
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
    """Per-value comparative stats within df. Deterministic sort inside."""
    n_all = df.height
    if n_all == 0:
        return []
    g_standout = df.filter(pl.col("segment") == "standout").height / n_all
    g_underperf = df.filter(pl.col("segment") == "underperformer").height / n_all
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
            pl.when(g_standout > 0).then(pl.col("standout_rate") / g_standout)
            .otherwise(None).alias("over_index"),
            pl.when(g_underperf > 0).then(pl.col("underperf_rate") / g_underperf)
            .otherwise(None).alias("underperf_over_index"),
        )
        .sort(
            ["over_index", "n_total", "value"],
            descending=[True, True, False],
            nulls_last=True,
        )
    )
    return [
        {
            "value": r["value"],
            "n_total": int(r["n_total"]),
            "standout_n": int(r["standout_n"]),
            "standout_rate": float(r["standout_rate"]),
            "underperf_n": int(r["underperf_n"]),
            "underperf_rate": float(r["underperf_rate"]),
            "over_index": float(r["over_index"]) if r["over_index"] is not None else None,
            "underperf_over_index": (
                float(r["underperf_over_index"])
                if r["underperf_over_index"] is not None else None
            ),
        }
        for r in agg.iter_rows(named=True)
    ]


def md_table(rows: list[dict], title: str) -> list[str]:
    prefix = "" if title.startswith("#") else "### "
    lines = [
        f"{prefix}{title}",
        "",
        "| value | n_total | standout_n | standout_rate | underperf_n "
        "| underperf_rate | over_index | underperf_over_index |",
        "|---|---|---|---|---|---|---|---|",
    ]
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


def decision_lines(rows: list[dict]) -> list[str]:
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

    lines = ["#### Decision rows", ""]
    lines.append(
        f"**Best-replicate** (n≥{DECISION_CELL}, over_index≥{OVER_INDEX_BAR}, "
        "underperf_over_index≤1.0): " + (
            "; ".join(
                f"`{r['value']}` (oi {r['over_index']:.2f}, uoi {r['underperf_over_index']:.2f}, n={r['n_total']})"  # noqa: E501
                for r in best[:5]
            ) if best else "none"
        )
    )
    lines.append("")
    lines.append(
        f"**Avoid** (n≥{DECISION_CELL}, underperf_over_index≥{OVER_INDEX_BAR}): " + (
            "; ".join(
                f"`{r['value']}` (uoi {r['underperf_over_index']:.2f}, oi {r['over_index']:.2f}, n={r['n_total']})"  # noqa: E501
                for r in avoid[:5]
            ) if avoid else "none"
        )
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
        df = pl.from_arrow(con.execute(ATTRIBUTION_SQL).arrow())
        obs_owner_n, obs_row_n = con.execute(
            "SELECT COUNT(DISTINCT owner_id), COUNT(*) "
            "FROM silver_ig_profile_observations"
        ).fetchone()
    finally:
        con.close()

    df = segment_frame(df)
    n_total = df.height
    n_attributed = df.filter(pl.col("follower_tier") != "unknown").height
    coverage = n_attributed / n_total if n_total else 0.0

    # GLOBAL rates for the tier-level comparison: whole labeled population.
    g_standout = df.filter(pl.col("segment") == "standout").height / n_total if n_total else 0.0
    g_underperf = df.filter(pl.col("segment") == "underperformer").height / n_total if n_total else 0.0  # noqa: E501

    lines = [
        "# Follower-tier EDA — comparative over-index by owner follower level",
        "",
        f"- label_version held constant: **{version}**",
        f"- posts analyzed: {n_total}",
        f"- coverage: {n_attributed}/{n_total} posts ({coverage:.1%}) have an owner "
        f"follower level attributed from silver_ig_profile_observations "
        f"({obs_owner_n} owners, {obs_row_n} observations).",
        f"- **global_standout_rate = {g_standout:.1%}**, "
        f"**global_underperf_rate = {g_underperf:.1%}** (whole labeled population, "
        "computed from this frame). Over-index 1.0 = population average.",
        "- `over_index` = standout_rate / global_standout_rate; "
        "`underperf_over_index` = underperf_rate / global_underperf_rate. "
        "Computed WITHIN each follower tier's attributed posts.",
        "- **Follower GROWTH over time is not yet available.** Observations come from a "
        "sparse bronze backfill (~58 rows / ~50 owners / 6 files); most owners have a "
        "single observation, so tier is a SNAPSHOT, not a trajectory. The "
        "0→100→1k→10k threshold-crossing mechanics remain web-first until forward "
        "scrapes accumulate a real time series.",
        "- Attribution: observation nearest at-or-after the post timestamp "
        "(owner_id match, owner_username fallback); it is a level-around-the-post, "
        "not a strictly at-post-time measure.",
        f"- Primary tables: n_total ≥ {MIN_CELL}; thin cells (n < {THIN_CELL}) flagged.",
        "",
    ]

    # ---- Tier-level over-index: is the tier itself over/under-indexing? ----
    tier_rows: list[dict] = []
    for tier in TIERS:
        t = df.filter(pl.col("follower_tier") == tier)
        n = t.height
        so = t.filter(pl.col("segment") == "standout").height
        up = t.filter(pl.col("segment") == "underperformer").height
        tier_rows.append({
            "value": tier,
            "n_total": n,
            "standout_n": so,
            "standout_rate": so / n if n else 0.0,
            "underperf_n": up,
            "underperf_rate": up / n if n else 0.0,
            "over_index": (so / n) / g_standout if n and g_standout else 0.0,
            "underperf_over_index": (up / n) / g_underperf if n and g_underperf else 0.0,
        })
    write_csv(
        out_dir / "follower_tier__segment_counts.csv",
        [
            (r["value"], r["n_total"], r["standout_n"], round(r["standout_rate"], 6),
             r["underperf_n"], round(r["underperf_rate"], 6),
             round(r["over_index"], 4), round(r["underperf_over_index"], 4))
            for r in tier_rows
        ],
        ["follower_tier", "n_posts", "standout_n", "standout_rate",
         "underperf_n", "underperf_rate", "over_index", "underperf_over_index"],
    )
    lines += [
        "## Tier-level over-index (all posts, vs whole labeled population)",
        "",
        "| follower tier | n_total | standout_n | standout_rate | underperf_n "
        "| underperf_rate | over_index | underperf_over_index |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in tier_rows:
        thin = " ⚠thin" if 0 < r["n_total"] < THIN_CELL else ""
        lines.append(
            f"| {r['value']}{thin} | {r['n_total']} | {r['standout_n']} "
            f"| {r['standout_rate']:.1%} | {r['underperf_n']} "
            f"| {r['underperf_rate']:.1%} "
            f"| {r['over_index']:.2f} | {r['underperf_over_index']:.2f} |"
        )
    lines.append("")

    # ---- Axis x tier comparative tables (attributed posts only) ----
    axes = [
        "gold_topic", "content_type", "format", "style", "admiralty",
        "is_educational", "is_actionable", "has_engagement_bait",
    ]
    attributed = df.filter(pl.col("follower_tier") != "unknown")
    attr_rows: list[tuple] = []
    for axis in axes:
        lines.append(f"## {axis} by follower tier (attributed posts only, sorted by over_index)")
        lines.append("")
        for tier in TIERS[:-1]:
            cell = attributed.filter(pl.col("follower_tier") == tier)
            n_cell = cell.height
            if n_cell == 0:
                continue
            rows = over_index_rows(cell, axis)
            for r in rows:
                attr_rows.append((
                    axis, tier, r["value"], r["n_total"],
                    round(r["standout_rate"], 6), round(r["underperf_rate"], 6),
                    round(r["over_index"], 4) if r["over_index"] is not None else "",
                    round(r["underperf_over_index"], 4) if r["underperf_over_index"] is not None else "",  # noqa: E501
                ))
            primary = [r for r in rows if r["n_total"] >= MIN_CELL]
            lines.append(f"### tier = {tier} (n_posts={n_cell})")
            lines.append("")
            if primary:
                lines += md_table(
                    primary,
                    f"#### values with n ≥ {MIN_CELL} (top by over_index)",
                )
                lines += decision_lines(rows)
            else:
                lines.append(f"_no values reach n ≥ {MIN_CELL} in this tier (largest cell: "
                             f"{max((r['n_total'] for r in rows), default=0)})_")
                lines.append("")
            long_tail = [r for r in rows if THIN_CELL <= r["n_total"] < MIN_CELL]
            if long_tail:
                lines += md_table(
                    long_tail[:LONG_TAIL_N],
                    f"#### long tail (top {LONG_TAIL_N} by over_index, {THIN_CELL} ≤ n < {MIN_CELL})",  # noqa: E501
                )
        lines.append("")
    md_path = out_dir / "follower_tier.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    write_csv(
        out_dir / "follower_tier__content_axis.csv",
        attr_rows,
        ["axis", "follower_tier", "value", "n", "standout_rate", "underperf_rate",
         "over_index", "underperf_over_index"],
    )


    print(
        f"label_version={version} posts={n_total} attributed={n_attributed} "
        f"({coverage:.1%}); obs: {obs_owner_n} owners / {obs_row_n} rows"
    )
    print(f"wrote {md_path} and CSVs under {out_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
