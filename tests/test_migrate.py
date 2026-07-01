"""Tests for the migration script.

Runs Phase 1 (bronze NDJSON -> Parquet) under various conditions.
Each test sandboxes its output via a tmp_path lake_dir.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def old_data_dir(tmp_path) -> Path:
    """Create a fake ig_pipeline data directory with bronze NDJSON."""
    bronze_dir = tmp_path / "bronze" / "datasets"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        {"id": "1", "shortCode": "abc", "caption": "Post one",
         "ownerUsername": "user1", "likesCount": 10,
         "commentsCount": 2, "timestamp": "2024-01-01T00:00:00.000Z"},
        {"id": "2", "shortCode": "def", "caption": "Post two",
         "ownerUsername": "user2", "likesCount": 20,
         "commentsCount": 3, "timestamp": "2024-01-02T00:00:00.000Z"},
        {"id": "3", "shortCode": "ghi", "caption": "Post three",
         "ownerUsername": "user3", "likesCount": 30,
         "commentsCount": 4, "timestamp": "2024-01-03T00:00:00.000Z"},
    ]
    ndjson_path = bronze_dir / "ds_001.jsonl"
    ndjson_path.write_text(
        "\n".join(json.dumps(r) for r in rows),
    )

    meta_path = bronze_dir / "ds_001.jsonl.meta"
    meta_path.write_text(json.dumps({
        "dataset_id": "ds_001",
        "run_id": "run_001",
        "actor": "apify~instagram-scraper",
        "item_count": 3,
        "downloaded_at": "2024-06-01T00:00:00Z",
    }))
    return tmp_path


@pytest.fixture
def old_data_malformed(tmp_path) -> Path:
    """Create bronze NDJSON with one malformed JSON line."""
    bronze_dir = tmp_path / "bronze" / "datasets"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    ndjson_path = bronze_dir / "ds_malformed.jsonl"
    ndjson_path.write_text(
        '{"id": "1", "shortCode": "abc", "likesCount": 10}\n'
        "this is not valid json\n"
        '{"id": "2", "shortCode": "def", "likesCount": 20}\n'
    )
    return tmp_path


@pytest.fixture
def empty_data_dir(tmp_path) -> Path:
    """Create empty bronze directory (nothing to migrate)."""
    (tmp_path / "bronze" / "datasets").mkdir(parents=True, exist_ok=True)
    return tmp_path


# ── Helper ────────────────────────────────────────────────────────────────

def _run_phase1(data_dir: Path, lake_dir: Path):
    """Execute migration Phase 1, writing output to lake_dir."""
    from scripts import migrate_from_ig_pipeline as migrate

    with patch.object(migrate, "_OLD_DATA_DIR", data_dir):
        with patch.object(
            migrate, "_OLD_BRONZE_DIR", data_dir / "bronze" / "datasets",
        ):
            with patch("datalake.defs.common.lake.BRONZE_LAKE", lake_dir):
                marker = data_dir / ".migration_complete"
                if marker.exists():
                    marker.unlink()
                migrate.phase1()


# ── Tests ─────────────────────────────────────────────────────────────────

def test_phase1_ndjson_to_parquet(old_data_dir, tmp_path):
    """GIVEN old NDJSON files
    WHEN migration Phase 1 runs
    THEN Parquet files written with correct row counts
    AND .meta sidecar copied alongside
    """
    _run_phase1(old_data_dir, tmp_path)

    dest = tmp_path / "ds_001.parquet"
    assert dest.exists(), "Parquet file not created"
    df = pl.read_parquet(dest)
    assert len(df) == 3
    assert list(df["shortCode"].to_list()) == ["abc", "def", "ghi"]

    meta_path = dest.with_suffix(".parquet.meta")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["dataset_id"] == "ds_001"
    assert meta["run_id"] == "run_001"
    assert meta["item_count"] == 3


def test_idempotent_rerun(old_data_dir, tmp_path):
    """GIVEN migration already ran
    WHEN re-run
    THEN existing Parquet is not overwritten
    """
    _run_phase1(old_data_dir, tmp_path)
    dest = tmp_path / "ds_001.parquet"
    mtime_before = dest.stat().st_mtime

    _run_phase1(old_data_dir, tmp_path)

    mtime_after = dest.stat().st_mtime
    assert mtime_before == mtime_after, "File was re-written"


def test_empty_source_dir(empty_data_dir, tmp_path):
    """GIVEN no NDJSON files
    WHEN migration runs
    THEN exits cleanly with no files created
    """
    _run_phase1(empty_data_dir, tmp_path)
    lake = list(tmp_path.iterdir())
    parquet = [f for f in lake if f.suffix == ".parquet"]
    assert not parquet


def test_malformed_json(old_data_malformed, tmp_path):
    """GIVEN NDJSON with unparseable lines
    WHEN migration runs Phase 1
    THEN the corrupt file is skipped (no partial data)
    """
    _run_phase1(old_data_malformed, tmp_path)
    dest = tmp_path / "ds_malformed.parquet"
    assert not dest.exists(), "corrupt NDJSON should be skipped entirely"
