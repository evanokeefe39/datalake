"""Consolidate duplicate auto-creators into curated creator identities.

The owner-profile backfill (``migrate_owner_profiles.py``) keyed creators by
``owner_username``, so curated creators without in-lake handles at migration
time got duplicate auto-creators owning their real profiles:

====================  ====  ==================  ====
Curated creator         id  Duplicate creator     id
====================  ====  ==================  ====
WAVIBOY                 21  bywaviboy             243
Nick Vinny              147  vinny_creative        610
====================  ====  ==================  ====

This script merges each duplicate into its curated identity:

1. Reassigns the profile (``profiles.creator_id``) to the curated creator,
   preserving the handle and the ad-hoc ``results_limit = -1`` sentinel.
2. Records the merge in ``creator_merges`` (additive, cataloged in
   ``schemas.py``) — the audit + reversal ledger.
3. Retires the duplicate creator row (DELETE) — nothing references it once the
   profile is reassigned. The original id+name are preserved in the ledger so
   ``--undo`` restores identity exactly.
4. Refreshes the mutable creator link on current ``dim_profile`` rows in
   state.duckdb (same in-place semantics as the ``profile_dimension`` asset; a
   later Dagster materialize re-affirms idempotently).

**Handle attribution is a human identity decision.** The default mapping below
is surfaced in the plan and PR for sign-off. Every step is idempotent and
reversible: ``--undo`` re-inserts the retired creators, re-points the profiles,
and stamps ``reversed_at`` in the ledger.

Usage::

    uv run python scripts/migrate_curated_creator_merge.py           # merge
    uv run python scripts/migrate_curated_creator_merge.py --undo    # reverse
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datalake.defs.common.schemas import sqlite_ddl_for

logger = logging.getLogger("migrate_curated_creator_merge")

DEFAULT_OPS = Path("data/ops.sqlite")
DEFAULT_DUCKDB = Path("data/state.duckdb")

# (merged_creator_id, surviving_creator_id, handle) — see PR/plan for the
# attribution rationale. Reversal data (name) is read from the creators row.
DEFAULT_MERGES: list[tuple[int, int, str]] = [
    (243, 21, "bywaviboy"),  # WAVIBOY — "byWAVIBOY" brand, high confidence
    (610, 147, "vinny_creative"),  # Nick Vinny — only in-lake candidate
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_ledger(con: sqlite3.Connection) -> None:
    con.execute(sqlite_ddl_for("creator_merges"))


def _get_creator(con: sqlite3.Connection, creator_id: int) -> tuple[int, str] | None:
    row = con.execute(
        "SELECT id, name FROM creators WHERE id = ?", [creator_id]
    ).fetchone()
    return (row[0], row[1]) if row else None


def merge(
    ops_path: Path,
    duckdb_path: Path,
    merges: list[tuple[int, int, str]],
) -> list[dict]:
    """Merge duplicate creators into curated identities. Returns the ledger."""
    con = sqlite3.connect(str(ops_path))
    results: list[dict] = []
    try:
        _ensure_ledger(con)
        for merged_id, surviving_id, handle in merges:
            merged = _get_creator(con, merged_id)
            surviving = _get_creator(con, surviving_id)
            if surviving is None:
                raise SystemExit(
                    f"Surviving creator {surviving_id} not found — aborting."
                )
            if merged is None:
                existing = con.execute(
                    "SELECT reversed_at, merged_at FROM creator_merges WHERE merged_creator_id = ?",
                    [merged_id],
                ).fetchone()
                if existing and existing[0] is None:
                    logger.info(
                        "Creator %s already merged into %s — skipping (idempotent).",
                        merged_id,
                        surviving_id,
                    )
                    results.append(
                        {"merged": merged_id, "surviving": surviving_id, "status": "already-merged"}
                    )
                    continue
                if existing and existing[0] is not None:
                    raise SystemExit(
                        f"Creator {merged_id} was merged then undone; re-merge "
                        f"requires the original row. Undo state at "
                        f"reversed_at={existing[0]}."
                    )
                raise SystemExit(f"Creator {merged_id} not found — aborting.")

            profile = con.execute(
                "SELECT creator_id FROM profiles WHERE platform='instagram' AND handle = ?",
                [handle],
            ).fetchone()
            if profile is None:
                raise SystemExit(f"Profile {handle!r} not found — aborting.")
            if profile[0] not in (merged_id, surviving_id):
                raise SystemExit(
                    f"Profile {handle!r} is owned by creator {profile[0]}, "
                    f"expected {merged_id} — refusing to reassign blindly."
                )

            now = _now_iso()
            # 1. Reassign the profile to the curated creator.
            con.execute(
                "UPDATE profiles SET creator_id = ?, updated_at = ? "
                "WHERE platform='instagram' AND handle = ?",
                [surviving_id, now, handle],
            )
            # 2. Ledger row (upsert: re-merge after an undo refreshes it).
            con.execute(
                "INSERT INTO creator_merges "
                "  (merged_creator_id, merged_creator_name, surviving_creator_id, handle, merged_at, reversed_at) "
                "VALUES (?, ?, ?, ?, ?, NULL) "
                "ON CONFLICT(merged_creator_id) DO UPDATE SET "
                "  merged_creator_name = excluded.merged_creator_name, "
                "  surviving_creator_id = excluded.surviving_creator_id, "
                "  handle = excluded.handle, merged_at = excluded.merged_at, "
                "  reversed_at = NULL",
                [merged_id, merged[1], surviving_id, handle, now],
            )
            # 3. Retire the duplicate (reversible via the ledger row). Refuse
            # if it still owns any other profile — that would orphan it.
            others = con.execute(
                "SELECT COUNT(*) FROM profiles WHERE creator_id = ?",
                [merged_id],
            ).fetchone()[0]
            if others:
                raise SystemExit(
                    f"Creator {merged_id} still owns {others} other profile(s); "
                    f"reassign them before retiring. Aborting."
                )
            con.execute("DELETE FROM creators WHERE id = ?", [merged_id])
            logger.info(
                "Merged creator %s (%s) into %s via profile %s.",
                merged_id,
                merged[1],
                surviving_id,
                handle,
            )
            results.append(
                {"merged": merged_id, "surviving": surviving_id, "status": "merged"}
            )
        con.commit()
    finally:
        con.close()

    _refresh_dim_profile(duckdb_path, ops_path, merges)
    return results


def undo(ops_path: Path, duckdb_path: Path, merges: list[tuple[int, int, str]]) -> None:
    """Reverse merges: restore retired creators, re-point profiles."""
    con = sqlite3.connect(str(ops_path))
    try:
        _ensure_ledger(con)
        for merged_id, _surviving_id, handle in merges:
            row = con.execute(
                "SELECT merged_creator_name, surviving_creator_id, reversed_at "
                "FROM creator_merges WHERE merged_creator_id = ?",
                [merged_id],
            ).fetchone()
            if row is None:
                logger.info("No merge recorded for creator %s — skipping.", merged_id)
                continue
            name, surviving_id, reversed_at = row
            if reversed_at is not None:
                logger.info("Creator %s already restored — skipping.", merged_id)
                continue
            now = _now_iso()
            # Refuse to clobber post-merge manual reassignment of the profile.
            owner = con.execute(
                "SELECT creator_id FROM profiles WHERE platform='instagram' "
                "AND handle = ?",
                [handle],
            ).fetchone()
            if owner is None or owner[0] != surviving_id:
                raise SystemExit(
                    f"Profile {handle!r} is now owned by "
                    f"{owner[0] if owner else 'nobody'}, not the merged-from "
                    f"creator {surviving_id}; refusing to undo blindly."
                )
            # 1. Re-insert the retired creator with its original identity.
            con.execute(
                "INSERT OR IGNORE INTO creators (id, name, created_at, updated_at) "
                "VALUES (?, ?, ?, ?)",
                [merged_id, name, now, now],
            )
            # 2. Re-point the profile back to the retired creator.
            con.execute(
                "UPDATE profiles SET creator_id = ?, updated_at = ? "
                "WHERE platform='instagram' AND handle = ?",
                [merged_id, now, handle],
            )
            # 3. Stamp the ledger.
            con.execute(
                "UPDATE creator_merges SET reversed_at = ? WHERE merged_creator_id = ?",
                [now, merged_id],
            )
            logger.info(
                "Restored creator %s (%s); profile %s re-pointed from %s.",
                merged_id,
                name,
                handle,
                surviving_id,
            )
        con.commit()
    finally:
        con.close()

    _refresh_dim_profile(duckdb_path, ops_path, merges)

def _refresh_dim_profile(
    duckdb_path: Path,
    ops_path: Path,
    merges: list[tuple[int, int, str]],
) -> None:
    """Refresh the mutable creator link on current dim_profile rows.

    Mirrors ``profile_dimension``: creator_id/creator_name is a mutable
    relationship, updated in place (no new SCD2 row). Resolves the creator
    name from ops at run time so it is correct for both merge and undo.
    """
    ops = sqlite3.connect(f"file:{ops_path}?mode=ro", uri=True)
    links: dict[str, tuple[int, str]] = {}
    try:
        for _merged_id, _surviving_id, handle in merges:
            row = ops.execute(
                "SELECT c.id, c.name FROM profiles p JOIN creators c "
                "  ON c.id = p.creator_id WHERE p.platform='instagram' "
                "  AND p.handle = ?",
                [handle],
            ).fetchone()
            if row:
                links[handle] = (row[0], row[1])
    finally:
        ops.close()

    con = duckdb.connect(str(duckdb_path))
    try:
        for handle, (creator_id, creator_name) in links.items():
            con.execute(
                "UPDATE dim_profile SET creator_id = ?, creator_name = ? "
                "WHERE owner_username = ? AND is_current = TRUE",
                [creator_id, creator_name, handle],
            )
            logger.info(
                "dim_profile refreshed: %s -> creator %s (%s).",
                handle,
                creator_id,
                creator_name,
            )
    finally:
        con.close()

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops", type=Path, default=DEFAULT_OPS)
    parser.add_argument("--duckdb", type=Path, default=DEFAULT_DUCKDB)
    parser.add_argument("--undo", action="store_true", help="Reverse the merges.")
    args = parser.parse_args()

    if args.undo:
        undo(args.ops, args.duckdb, DEFAULT_MERGES)
    else:
        merge(args.ops, args.duckdb, DEFAULT_MERGES)
        logger.info("Consolidation complete.")


if __name__ == "__main__":
    sys.exit(main())
