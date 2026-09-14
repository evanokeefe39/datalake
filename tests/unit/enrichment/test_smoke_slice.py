"""tests for scripts/make_smoke_slice.py — the deterministic dev/smoke slice.

The slice builder reads the LIVE lake read-only and writes ONLY under its
explicit ``--out`` root. These tests build against tmp roots and assert the
live roots are untouched (content hashes + file inventories unchanged).
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts.make_smoke_slice import (  # noqa: E402
    LIVE_BRONZE,
    LIVE_OPS_DB,
    LIVE_STATE_DB,
    build_slice,
    enrichment_plan_check,
)

SIX_SILVER = (
    "silver_visual_annotations",
    "silver_visual_summaries",
    "silver_audio_transcripts",
    "silver_text_annotations",
    "silver_text_summaries",
    "silver_content_classification",
)

def _snapshot_live() -> dict[str, str]:
    """Fingerprint of everything the slice must never touch.

    stat-based (size + mtime_ns) rather than content hashes: the live DuckDB
    file can be exclusively locked by another writer at test time, and any
    write to it changes size/mtime. Directory walks cover data/lake/bronze.
    """
    snap: dict[str, str] = {}
    for db in (LIVE_STATE_DB, LIVE_OPS_DB):
        st = db.stat()
        snap[str(db)] = f"{st.st_size}:{st.st_mtime_ns}"
    for f in sorted(LIVE_BRONZE.rglob("*")):
        if f.is_file():
            st = f.stat()
            snap[str(f)] = f"{st.st_size}:{st.st_mtime_ns}"
    return snap

@pytest.fixture(scope="module")
def live_snapshot():
    return _snapshot_live()


@pytest.fixture(scope="module")
def slice_a(tmp_path_factory, live_snapshot):
    out = tmp_path_factory.mktemp("slice_a")
    build_slice(out, creators=3, posts=12, seed=42)
    return out


def test_live_roots_untouched(live_snapshot, slice_a):
    assert _snapshot_live() == live_snapshot, "the smoke slice wrote to the live lake"


def test_deterministic_selection(tmp_path_factory, live_snapshot, slice_a):
    out_b = tmp_path_factory.mktemp("slice_b")
    build_slice(out_b, creators=3, posts=12, seed=42)
    a = duckdb.connect(str(slice_a / "state.duckdb"), read_only=True)
    b = duckdb.connect(str(out_b / "state.duckdb"), read_only=True)
    ids_a = [r[0] for r in a.execute("SELECT post_id FROM silver_ig_posts ORDER BY post_id").fetchall()]
    ids_b = [r[0] for r in b.execute("SELECT post_id FROM silver_ig_posts ORDER BY post_id").fetchall()]
    a.close()
    b.close()
    assert ids_a == ids_b and len(ids_a) == 12


def test_six_silver_tables_and_classification(slice_a):
    conn = duckdb.connect(str(slice_a / "state.duckdb"), read_only=True)
    names = {
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        )
    }
    assert set(SIX_SILVER) <= names
    n_posts, n_cls = conn.execute(
        "SELECT count(*), "
        "(SELECT count(*) FROM silver_content_classification c WHERE c.post_id = p.post_id) "
        "FROM silver_ig_posts p"
    ).fetchone()
    conn.close()
    assert n_posts == 12
    assert n_cls == 12, "the slice must inherit the migrated classification for every post"


def test_media_bytes_present_and_no_dangling_paths(slice_a):
    ops = sqlite3.connect(str(slice_a / "ops.sqlite"))
    rows = ops.execute("SELECT cache_key, local_path FROM media_cache").fetchall()
    ops.close()
    assert rows, "media_cache must carry the selected posts' URLs"
    media_root = slice_a / "media"
    for cache_key, local_path in rows:
        assert str(slice_a) in local_path, f"local_path not rewritten to smoke root: {local_path}"
        assert Path(local_path).exists(), f"dangling local_path: {local_path}"
        assert cache_key in Path(local_path).name
    assert any(media_root.rglob("*")), "media bytes must exist under the smoke root"


def test_stratification(slice_a):
    conn = duckdb.connect(str(slice_a / "state.duckdb"), read_only=True)
    videos, carousels, text_only = conn.execute(
        "SELECT "
        "count(*) FILTER (WHERE video_view_count > 0), "
        "count(*) FILTER (WHERE media_count > 1), "
        "count(*) FILTER (WHERE media_files = '[]') FROM silver_ig_posts"
    ).fetchone()
    labels = {
        r[0] for r in conn.execute("SELECT DISTINCT label FROM ig_post_labels")
    }
    conn.close()
    assert videos >= 1 and carousels >= 1 and text_only >= 1
    assert len(labels) >= 2, "standout/non-standout mix required"


def test_enrichment_plan_submittable(slice_a):
    sub = enrichment_plan_check(slice_a, ["visual", "text"])
    assert sub["visual"] > 0, "visual pass must have submittable items — that is the slice's purpose"
    assert sub["text"] > 0


def test_readme_declares_provenance(slice_a):
    readme = (slice_a / "README.md").read_text(encoding="utf-8")
    assert "NOT the lake" in readme
    assert "make_smoke_slice.py" in readme
    assert "conform" in readme


def test_cli_end_to_end(tmp_path, live_snapshot):
    """One command builds the slice and its usability check passes."""
    out = tmp_path / "smoke_cli"
    proc = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "make_smoke_slice.py"),
            "--posts", "6",
            "--creators", "2",
            "--seed", "7",
            "--out", str(out),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "usability check: PASS" in proc.stdout
    assert (out / "state.duckdb").exists()
    assert (out / "ops.sqlite").exists()
    assert (out / "README.md").exists()
    assert _snapshot_live() == live_snapshot, "CLI run touched the live lake"
    # Overwrite guard: the default live roots are refused.
    bad = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "make_smoke_slice.py"),
            "--out", str(REPO_ROOT / "data" / "lake"),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert bad.returncode != 0
    assert "REFUSING" in bad.stdout + bad.stderr
