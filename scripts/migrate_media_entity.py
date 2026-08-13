"""Migrate live DBs for media cache + entity routing (feat/media-and-entity-routing).

Applies the schema delta the readiness test (tests/operational/test_state_compatibility.py)
asserts:

- ops.sqlite: create ``media_cache`` (byte cache), drop the vestigial
  ``instagram_media_cache`` (URL cache), add ``media_metadata.expires_at``.
- state.duckdb: create ``silver_ig_profiles`` and ``silver_ig_comments``,
  add ``dim_profile.profile_pic_path``.

Idempotent — safe to re-run.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DUCKDB_PATH = Path("data/state.duckdb")
OPS_PATH = Path("data/ops.sqlite")


def _backup(path: Path) -> Path | None:
    """Copy a DB file to a timestamped backup before destructive changes."""
    if not path.exists():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(f".media_entity_bak_{ts}{path.suffix}")
    backup.write_bytes(path.read_bytes())
    print(f"Backed up {path} -> {backup}")
    return backup


def migrate() -> None:
    # ── ops.sqlite ─────────────────────────────────────────────────────
    if OPS_PATH.exists():
        _backup(OPS_PATH)
        con = sqlite3.connect(str(OPS_PATH))
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS media_cache (
                    cache_key    TEXT PRIMARY KEY,
                    local_path   TEXT NOT NULL,
                    content_type TEXT,
                    size_bytes   INTEGER,
                    fetched_at   TEXT NOT NULL,
                    source_url   TEXT
                )
                """
            )
            con.execute("DROP TABLE IF EXISTS instagram_media_cache")
            # media_metadata.expires_at was added to the catalog but never
            # migrated to the live DB (multimodal Phase 2).
            try:
                con.execute(
                    "ALTER TABLE media_metadata ADD COLUMN expires_at TEXT"
                )
            except sqlite3.OperationalError:
                pass  # column already exists
            con.commit()
            print(
                "ops.sqlite: created media_cache, dropped instagram_media_cache, "
                "added media_metadata.expires_at"
            )
        finally:
            con.close()
    else:
        print("ops.sqlite not found — skipping")

    # ── state.duckdb ───────────────────────────────────────────────────
    if DUCKDB_PATH.exists():
        _backup(DUCKDB_PATH)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_ig_profiles (
                    owner_id        TEXT PRIMARY KEY,
                    owner_username  TEXT,
                    full_name       TEXT,
                    biography       TEXT,
                    followers_count INTEGER,
                    follows_count   INTEGER,
                    posts_count     INTEGER,
                    is_business     BOOLEAN,
                    is_verified     BOOLEAN,
                    profile_pic_url TEXT,
                    external_url    TEXT,
                    source_dataset  TEXT,
                    processed_on    TIMESTAMP
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_ig_comments (
                    comment_id     TEXT PRIMARY KEY,
                    post_id        TEXT,
                    post_shortcode TEXT,
                    text           TEXT,
                    owner_username TEXT,
                    owner_id       TEXT,
                    likes_count    INTEGER,
                    timestamp      TIMESTAMP,
                    reply_to_id    TEXT,
                    source_dataset TEXT,
                    processed_on   TIMESTAMP
                )
                """
            )
            # DuckDB ALTER has no IF NOT EXISTS — tolerate duplicate column.
            try:
                con.execute("ALTER TABLE dim_profile ADD COLUMN profile_pic_path TEXT")
            except Exception:
                pass
            print(
                "state.duckdb: created silver_ig_profiles, silver_ig_comments; "
                "added dim_profile.profile_pic_path"
            )
        finally:
            con.close()
    else:
        print("state.duckdb not found — skipping")


if __name__ == "__main__":
    migrate()
