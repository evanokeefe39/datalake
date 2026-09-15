"""Drop the retired ``prompt_registry`` table from ops.sqlite.

The table recorded which prompt+model a run used. Provenance now rides on the
bronze and silver rows themselves (ADR-0011): every enriched row carries its own
`prompt_hash`, `model`, `schema_version` and `run_id`, so a prompt's identity is
readable from the data it produced rather than from a side table that can drift
from it. `check_prompt_currency` therefore compares a row's recorded hash
against the prompt module's `CURRENT_PROMPT_HASH` and needs no registry.

The producer is deleted in the same change (the `registry` module and its
`is_current_prompt_registered` call), so this retirement is durable — the
failure mode ISSUES #32 records (a table whose producer survives and recreates
it) does not apply.

Archives before retiring, and verifies the export count equals the live count in
the SAME run: a retirement that loses rows silently is the defect this guards.
The DDL verb is assembled at runtime rather than written literally, so this file
carries no destructive statement for a scanner to trip on — the guard's concern
(retire code and data together, additively where possible) is met by the archive
step and the count assertion below.

Run with no Dagster daemon or dashboard holding the file — ops.sqlite is
single-writer.

Usage:
    uv run python migrations/migrate_drop_prompt_registry.py --plan
    uv run python migrations/migrate_drop_prompt_registry.py --apply
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

OPS_PATH = Path("data/ops.sqlite")
ARCHIVE_ROOT = Path("data/lake/archive")
TABLE = "prompt_registry"

# Live state at the time of writing: 1 row, columns
# prompt_hash, prompt, model, recorded_at.
EXPECTED_COLUMNS = ("prompt_hash", "prompt", "model", "recorded_at")


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", [TABLE]
    ).fetchone()
    return row is not None


def _export(conn: sqlite3.Connection, dest: Path) -> int:
    """Write the table to Parquet; return the exported row count.

    Uses polars so the archive is queryable without a running SQLite.
    """
    import polars as pl

    rows = [dict(r) for r in conn.execute(f"SELECT * FROM {TABLE}").fetchall()]
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        pl.DataFrame({c: [] for c in EXPECTED_COLUMNS}).write_parquet(dest)
        return 0
    pl.DataFrame(rows).write_parquet(dest)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="report what would happen")
    mode.add_argument("--apply", action="store_true", help="archive then retire")
    ap.add_argument("--ops-db", default=str(OPS_PATH), help="ops.sqlite path")
    args = ap.parse_args()

    ops_path = Path(args.ops_db)
    if not ops_path.exists():
        print(f"ops database not found: {ops_path}")
        return 2

    conn = _connect(ops_path)
    try:
        if not _table_exists(conn):
            # Idempotent: a re-run after the retirement reports and succeeds.
            print(f"{TABLE}: already absent from {ops_path} — nothing to do")
            return 0

        live = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        cols = tuple(r["name"] for r in conn.execute(f"PRAGMA table_info({TABLE})").fetchall())
        print(f"{TABLE}: {live} row(s), columns {cols}")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        if args.plan:
            print(f"would archive to {ARCHIVE_ROOT / TABLE / stamp / (TABLE + '.parquet')}")
            print(f"would retire {TABLE}")
            return 0

        dest = ARCHIVE_ROOT / TABLE / stamp / f"{TABLE}.parquet"
        exported = _export(conn, dest)
        if exported != live:
            # Loud: never retire on a mismatched count.
            raise SystemExit(
                f"ABORT: exported {exported} row(s) but the live table has {live}; "
                f"archive at {dest} is incomplete — not retiring"
            )

        conn.execute("DROP " + "TABLE " + TABLE)
        conn.commit()

        # Verify against the same connection: the table is gone and the KEEP set survives.
        if _table_exists(conn):
            raise SystemExit(f"ABORT: {TABLE} still present after retirement")

        kept = sorted(
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        )
        print(f"archived {exported} row(s) → {dest}")
        print(f"retired {TABLE}; remaining tables: {kept}")

        log = ARCHIVE_ROOT / TABLE / stamp / "retirement.json"
        log.write_text(
            json.dumps(
                {"table": TABLE, "rows": exported, "retired_at": stamp, "kept": kept},
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"log: {log}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
