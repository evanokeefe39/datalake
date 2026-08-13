"""State inspection and reset helpers for the CLI."""

from __future__ import annotations

import sqlite3
from datetime import datetime

import duckdb

DB_PATH = "data/state.duckdb"
OPS_PATH = "data/ops.sqlite"
DEFAULT_RESET_DATE = "1970-01-02"


def parse_datetime(value: str) -> datetime:
    """Parse ISO 8601 datetime or date string to UTC datetime.

    Accepts: ``2026-06-15`` (date only → midnight UTC),
    ``2026-06-15T12:00``, ``2026-06-15T00:00:00Z``, ``2026-06-15T00:00:00+00:00``.
    """
    value = value.strip()
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        # Date only — append midnight UTC
        value = value + "T00:00:00+00:00"
    elif value.endswith("Z"):
        value = value[:-1] + "+00:00"
    elif "+" not in value and "-" not in value[10:]:
        # No timezone info — assume UTC
        value = value + "+00:00"
    return datetime.fromisoformat(value)


# ── Printers ──────────────────────────────────────────────────────────────


def print_full_state(phase: str) -> None:
    """Show all tables, watermarks, and ops tables."""
    print(f"\n{'='*60}")
    print(f"  {phase}")
    print(f"{'='*60}")

    db = duckdb.connect(DB_PATH, read_only=True)
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()
    for (t,) in tables:
        cnt = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t:25s} {cnt:>6} rows")

    wm = db.execute("SELECT name, timestamp FROM watermarks ORDER BY name").fetchall()
    if wm:
        print("  --- watermarks ---")
        for name, ts in wm:
            print(f"  {name:25s} {ts}")
    db.close()

    ops = sqlite3.connect(f"file:{OPS_PATH}?mode=ro", uri=True)
    ops_tables = ops.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for (t,) in ops_tables:
        cnt = ops.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  ops.{t:20s} {cnt:>6} rows")
    ops.close()


def print_batches() -> None:
    """Show batch_jobs with per-status item counts."""
    con = sqlite3.connect(f"file:{OPS_PATH}?mode=ro", uri=True)
    jobs = con.execute(
        "SELECT id, status, total_items, processed_items, failed_items, created_at "
        "FROM batch_jobs ORDER BY id"
    ).fetchall()
    if not jobs:
        print("  No batches found.")
    else:
        print(f"\n{'='*60}")
        print("  Batches")
        print(f"{'='*60}")
        for j in jobs:
            print(
                f"  batch #{j[0]}: status={j[1]}, total={j[2]}, "
                f"processed={j[3]}, failed={j[4]}, created={j[5][:19]}"
            )
            items = con.execute(
                "SELECT status, COUNT(*) FROM batch_items "
                "WHERE job_id=? GROUP BY status",
                [j[0]],
            ).fetchall()
            for status, cnt in items:
                print(f"    {status}: {cnt}")
    con.close()


def print_watermarks() -> None:
    """Show watermark rows."""
    db = duckdb.connect(DB_PATH, read_only=True)
    wm = db.execute("SELECT name, timestamp FROM watermarks ORDER BY name").fetchall()
    db.close()
    print(f"\n{'='*60}")
    print("  Watermarks")
    print(f"{'='*60}")
    if not wm:
        print("  None set.")
    else:
        for name, ts in wm:
            print(f"  {name:25s} {ts}")


# ── Resetters ─────────────────────────────────────────────────────────────


def reset_watermarks(since: datetime) -> None:
    db = duckdb.connect(DB_PATH)
    for name in ("silver_ig", "gold_ig"):
        db.execute(
            "INSERT INTO watermarks (name, timestamp) VALUES (?, ?) "
            "ON CONFLICT (name) DO UPDATE SET timestamp = excluded.timestamp",
            [name, since],
        )
    db.close()


def reset_batches() -> None:
    con = sqlite3.connect(OPS_PATH)
    con.execute("DELETE FROM batch_items")
    con.execute("DELETE FROM batch_jobs")
    con.commit()
    con.close()
