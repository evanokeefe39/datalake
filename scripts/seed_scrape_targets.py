"""Seed the tracked-profile list in ``scrape_targets`` from existing silver data.

Determines WHICH profiles to track and at WHAT depth (``results_limit``)
using what the pipeline already holds — no live scraping, no Gemini calls.

Seed rule — everything except the long tail:

- **seed** — a profile already in ``dim_profile`` (deliberately tracked), OR
  any owner with a track record (>= 2 posts), OR a single post with
  engagement volume >= 1,000,000 (a viral post, not noise).
- **skip** — an *untracked* single-post owner below the viral bar. These are
  the residue of the original wide scrape pulling "related" profiles.

Depth (``results_limit``) = the number of posts we already hold for that
profile. No tiers — every seeded profile gets the same treatment.

Engagement volume = real likes + comments + video views. A ``likes_count``
of ``-1`` is the scraper's sentinel for "like count hidden/unavailable" and
is treated as missing (not subtracted); absent values are NULL, never 0.

Usage:

    uv run python scripts/seed_scrape_targets.py            # report only (CSV + summary)
    uv run python scripts/seed_scrape_targets.py --write    # + upsert into scrape_targets

``--write`` upserts (never deletes), so manual entries not present in silver
are left untouched. It writes straight to ``ops.sqlite`` and does NOT trigger
live Apify scrapes — unlike the dashboard POST endpoint.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import duckdb

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.instagram.scrape_targets import ensure_schema, upsert_target

# ── Thresholds (named constants — tune here, not in the classification body) ──
VIRAL_VOLUME = 1_000_000
"""Engagement volume above which an untracked single post is worth tracking."""

DUCKDB_PATH = os.environ.get("IG_DB_PATH", "data/state.duckdb")
OPS_DB_PATH = os.environ.get("OPS_DB_PATH", "data/ops.sqlite")
REPORT_PATH = Path(os.environ.get("IG_DATA_DIR", "data")) / "seed_scrape_targets.csv"


def _engagement(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Per-owner post counts and NULL-aware engagement, joined to the tracked set.

    ``likes_sum`` excludes the ``-1`` sentinel (hidden likes) so it never
    deflates volume. ``avg_likes`` is NULL when no real likes were found.
    """
    return [
        dict(zip(
            ("username", "n_posts", "likes_sum", "comments_sum", "video_sum",
             "avg_likes", "tracked"),
            row,
        ))
        for row in con.execute(
            """
            WITH agg AS (
                SELECT owner_username,
                       COUNT(*) AS n,
                       COALESCE(SUM(NULLIF(likes_count, -1)), 0)           AS likes_sum,
                       COALESCE(SUM(comments_count), 0)                     AS comments_sum,
                       COALESCE(SUM(COALESCE(video_play_count, video_view_count)), 0) AS video_sum,
                       AVG(NULLIF(likes_count, -1))                         AS avg_likes
                FROM silver_ig_posts
                WHERE owner_username IS NOT NULL
                GROUP BY owner_username
            )
            SELECT a.owner_username, a.n, a.likes_sum, a.comments_sum, a.video_sum,
                   a.avg_likes,
                   (d.owner_username IS NOT NULL) AS tracked
            FROM agg a
            LEFT JOIN dim_profile d
                   ON d.owner_username = a.owner_username
                  AND d.is_current = TRUE
            """
        ).fetchall()
    ]


def classify(o: dict) -> dict:
    """Return a seed row, or ``None`` for long-tail single posts."""
    volume = o["likes_sum"] + o["comments_sum"] + o["video_sum"]
    if o["tracked"] or o["n_posts"] >= 2 or volume >= VIRAL_VOLUME:
        return {**o, "volume": volume, "results_limit": o["n_posts"]}
    return None


def _report(rows: list[dict]) -> None:
    """Write the seed list to CSV and print a summary."""
    seeded = [r for r in rows if r is not None]
    skipped = len(rows) - len(seeded)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["username", "results_limit", "n_posts", "avg_likes", "volume"]
    with REPORT_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(seeded, key=lambda r: (-r["results_limit"], r["username"])):
            writer.writerow({k: r.get(k) for k in fieldnames})

    print(f"owners in silver: {len(rows)}")
    print(f"seeded: {len(seeded)}   skipped (long-tail single posts): {skipped}")
    print(f"report written: {REPORT_PATH}")
    print()
    print("== seeded (depth = posts already held) ==")
    for r in sorted(seeded, key=lambda r: (-r["results_limit"], r["username"])):
        avg = "" if r["avg_likes"] is None else f"{r['avg_likes']:.0f}"
        print(f"  {r['username']:28s} depth={r['results_limit']:3d} "
              f"avg_likes={avg:>8s} volume={r['volume']}")


def _write(rows: list[dict]) -> None:
    """Upsert seed rows into ``scrape_targets`` (no live scrape)."""
    ops = SQLiteResource(database=OPS_DB_PATH)
    ensure_schema(ops)
    to_write = [r for r in rows if r is not None]
    for r in to_write:
        upsert_target(
            ops,
            username=r["username"],
            profile_url=f"https://www.instagram.com/{r['username']}/",
            results_type="details",
            results_limit=r["results_limit"],
            enabled=True,
        )
    print(f"upserted {len(to_write)} scrape targets (all enabled)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true",
                        help="upsert seed rows into scrape_targets")
    args = parser.parse_args()

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        rows = [classify(o) for o in _engagement(con)]
    finally:
        con.close()

    _report(rows)
    if args.write:
        _write(rows)


if __name__ == "__main__":
    main()
