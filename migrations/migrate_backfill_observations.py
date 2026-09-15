"""One-shot migration: backfill observation #1 for existing silver posts.

Builds the first ``silver_ig_post_observations`` row for every post already
in silver by reading the ORIGINAL bronze Parquet files (immutable). The
``observed_at`` provenance chain is: ``.meta`` sidecar ``downloaded_at`` →
bronze file mtime — the ORIGINAL scrape time, NEVER the backfill run time.

Reads bronze, not current silver (silver is only used read-only to bound
which post_ids are backfilled, mirroring the ingestion filter).

Idempotent — ``INSERT OR IGNORE`` on PK ``(post_id, source_dataset)`` means
re-runs add zero rows. Additive-only; never drops or alters tables.

Usage:
    uv run python scripts/migrate_backfill_observations.py           # backfill
    uv run python scripts/migrate_backfill_observations.py --dry-run # counts only
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from datalake.defs.common.schemas import duckdb_ddl

BRONZE_DIR = Path("data/lake/bronze")
DB_PATH = "data/state.duckdb"

logger = logging.getLogger("migrate_backfill_observations")


def _observed_at(fp: Path) -> datetime:
    """Original scrape time for a bronze file: meta.downloaded_at → mtime."""
    meta_path = fp.with_suffix(".parquet.meta")
    if meta_path.exists():
        try:
            raw = json.loads(meta_path.read_text(encoding="utf-8")).get("downloaded_at")
            if raw:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (json.JSONDecodeError, ValueError, OSError):
            pass
    return datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)


def _int(row: dict, *keys: str) -> int | None:
    for k in keys:
        if k in row and row[k] is not None:
            return int(row[k])
    return None


def collect_observations(silver_ids: set[str] | None) -> list[tuple]:
    """Scan bronze Parquet files for (post_id, observed_at, counts, dataset)."""
    rows: dict[tuple[str, str], tuple] = {}
    files = sorted(BRONZE_DIR.glob("*.parquet"))
    if not files:
        logger.info("No bronze files found.")
        return []

    for fp in files:
        try:
            df = pl.read_parquet(fp)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", fp.name, exc)
            continue
        cols = set(df.columns)
        # Posts entity sniff: same heuristic as _classify_bronze schema fallback.
        if not ("id" in cols and "shortCode" in cols):
            logger.info("Skipping %s — not a posts dataset", fp.name)
            continue
        observed_at = _observed_at(fp)
        dataset_id = fp.stem
        for row in df.iter_rows(named=True):
            post_id = row.get("id")
            if post_id is None:
                continue
            post_id = str(post_id)
            if silver_ids is not None and post_id not in silver_ids:
                continue
            key = (post_id, dataset_id)
            if key not in rows:
                rows[key] = (
                    post_id,
                    observed_at,
                    _int(row, "likesCount", "likes_count"),
                    _int(row, "commentsCount", "comments_count"),
                    _int(row, "videoViewCount", "video_view_count"),
                    _int(row, "videoPlayCount", "video_play_count"),
                    dataset_id,
                )
    return list(rows.values())


def apply_backfill(db: duckdb.DuckDBPyConnection, rows: list[tuple]) -> int:
    """Insert observation rows with PK dedup; returns rows actually added."""
    before = db.execute("SELECT COUNT(*) FROM silver_ig_post_observations").fetchone()[0]
    db.executemany(
        "INSERT OR IGNORE INTO silver_ig_post_observations VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    after = db.execute("SELECT COUNT(*) FROM silver_ig_post_observations").fetchone()[0]
    return after - before


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="show what would be backfilled"
    )
    args = parser.parse_args()

    db = duckdb.connect(DB_PATH)
    db.execute(duckdb_ddl("silver_ig_post_observations"))

    silver_ids = {r[0] for r in db.execute("SELECT post_id FROM silver_ig_posts").fetchall()}
    rows = collect_observations(silver_ids)
    print(f"\n  Silver posts:        {len(silver_ids)}")
    print(f"  Bronze observations: {len(rows)} (unique post_id × source_dataset)")

    if args.dry_run:
        print("  Dry run — no rows written.")
        db.close()
        return

    added = apply_backfill(db, rows)
    total = db.execute("SELECT COUNT(*) FROM silver_ig_post_observations").fetchone()[0]
    db.close()
    print(f"\n  Added: {added} rows (table total: {total})")


if __name__ == "__main__":
    main()
