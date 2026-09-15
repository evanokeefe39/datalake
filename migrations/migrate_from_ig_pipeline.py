"""Standalone migration script: ig_pipeline NDJSON → datalake Parquet.

Two-phase design:
  Phase 1 (default): bronze NDJSON files → typed Parquet via Polars.
  Phase 2 (``--phase2``): silver/gold data → DuckDB state tables.

Runs idempotently — re-running is safe. A ``.migration_complete`` marker
in the old data directory skips both phases on subsequent runs.

Usage:
    uv run python scripts/migrate_from_ig_pipeline.py
    uv run python scripts/migrate_from_ig_pipeline.py --phase2
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import polars as pl

# ── Allow import of datalake path helpers ─────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datalake.defs.common.lake import bronze_path  # noqa: E402

# ── Old ig_pipeline path (adjust to your checkout) ───────────────────────
_OLD_DATA_DIR = Path.home() / "repos" / "ig-pipeline" / "data"
_OLD_BRONZE_DIR = _OLD_DATA_DIR / "bronze" / "datasets"
_OLD_SILVER_DIR = _OLD_DATA_DIR / "silver" / "posts"
_OLD_GOLD_DIR = _OLD_DATA_DIR / "gold" / "analyses"
_MARKER = _OLD_DATA_DIR / ".migration_complete"

log = logging.getLogger("migrate")


# ── Helpers ───────────────────────────────────────────────────────────────

def _ndjson_files() -> list[Path]:
    """Return all bronze NDJSON files in the old data directory."""
    if not _OLD_BRONZE_DIR.is_dir():
        return []
    return sorted(_OLD_BRONZE_DIR.glob("*.jsonl"))


def _silver_post_dirs() -> list[Path]:
    """Return all silver post directories containing a post.json."""
    if not _OLD_SILVER_DIR.is_dir():
        return []
    return sorted(d for d in _OLD_SILVER_DIR.iterdir() if d.is_dir() and (d / "post.json").exists())


def _gold_analyses() -> list[Path]:
    """Return all gold enriched.json files."""
    if not _OLD_GOLD_DIR.is_dir():
        return []
    return sorted(_OLD_GOLD_DIR.glob("*.json"))


def _load_meta(ndjson_path: Path) -> dict | None:
    """Load the .meta sidecar if it exists."""
    meta_path = ndjson_path.with_suffix(".jsonl.meta")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except json.JSONDecodeError:
            log.warning("Corrupt meta file: %s", meta_path)
    return None


# ── Phase 1: Bronze NDJSON → Parquet ──────────────────────────────────────

def phase1() -> int:
    """Convert bronze NDJSON files to typed Parquet. Returns count processed."""
    files = _ndjson_files()
    if not files:
        log.info("No bronze NDJSON files found — nothing to migrate.")
        return 0

    processed = 0
    for ndjson_path in files:
        # Derive dataset_id from filename (e.g. "abc123.jsonl" → "abc123")
        dataset_id = ndjson_path.stem
        dest = bronze_path(dataset_id)

        if dest.exists():
            log.info("Skipping %s — already exists at %s", dataset_id, dest)
            continue

        try:
            df = pl.read_ndjson(ndjson_path, infer_schema_length=None)
        except Exception as exc:
            log.warning("Skipping %s — unparseable NDJSON: %s", ndjson_path.name, exc)
            continue

        df.write_parquet(dest)
        log.info("Wrote %s → %s (%d rows)", dataset_id, dest, len(df))

        # Copy .meta sidecar alongside Parquet (unchanged)
        meta = _load_meta(ndjson_path)
        if meta:
            meta_path = dest.with_suffix(".parquet.meta")
            meta_path.write_text(json.dumps(meta, indent=2))
            log.info("  Copied .meta sidecar (%s)", meta_path.name)
        else:
            log.info("  No .meta sidecar for %s", dataset_id)

        processed += 1

    return processed


# ── Phase 2: Silver/Gold → DuckDB ─────────────────────────────────────────

def phase2() -> int:
    """RETIRED 2026-09-15 (W9) — refuses; creates NO tables.

    Phase 2 upserted silver posts and ``gold_analyses`` into DuckDB. Both the
    table and the migration are retired:

    - ``gold_analyses`` was superseded by ``silver_content_classification`` and
      DROPPED by the W9 retirement (archived at
      ``data/lake/archive/gold_analyses/``). ISSUES.md #34.
    - This was one of several paths that would RESURRECT a retired table; the
      others were ``migrate_schema_drift.py``, ``migrate_to_v2.py`` and
      ``migrate_media_entity.py``.

    Refusing loudly (rather than deleting the function) keeps ``--phase2`` a
    valid, honest CLI surface: an operator who runs it gets told why it cannot
    run instead of silently getting a resurrected table.

    Phase 1 (NDJSON bronze import) is UNAFFECTED and still live — it is what
    ``tests/test_migrate.py`` covers.
    """
    raise SystemExit(
        "REFUSING: --phase2 is retired (W9, 2026-09-15). It wrote "
        "`gold_analyses`, which was dropped and superseded by "
        "`silver_content_classification`. Re-running it would resurrect a "
        "retired table. See ISSUES.md #34. Phase 1 (bronze import) is "
        "unaffected — run without --phase2."
    )


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Migrate ig_pipeline data to datalake")
    parser.add_argument("--phase2", action="store_true", help="Run Phase 2 (silver/gold → DuckDB)")
    args = parser.parse_args()

    if not _OLD_DATA_DIR.is_dir():
        log.info("Old data directory not found: %s — nothing to migrate", _OLD_DATA_DIR)
        return

    if _MARKER.exists():
        log.info("Migration already complete (.migration_complete exists) — skipping")
        return

    # Phase 1 — always runs by default
    p1 = phase1()

    # Phase 2 — gated by --phase2 flag
    if args.phase2:
        p2 = phase2()
    else:
        p2 = 0
        log.info("Phase 2 skipped (use --phase2 to run silver/gold migration)")

    # Write marker
    _MARKER.write_text(json.dumps({
        "bronze_files_processed": p1,
        "silver_gold_processed": p2,
        "note": "Migration complete. Delete this file to re-run.",
    }, indent=2))
    log.info("Migration complete. Marker written: %s", _MARKER)


if __name__ == "__main__":
    main()
