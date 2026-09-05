"""Dump the creator/roster list to a compact, git-committable CSV.

The full DBs (ops.sqlite/state.duckdb) are backed up off-machine to R2 by
backup_databases.py. This is the lightweight, human-readable redundancy: a
compact CSV of the operational roster (creators + profiles + merges) that is
small (a few KB vs 30-70 MB binaries) and versioned in git, so the creator list
survives machine loss even independent of the R2 backup, and is diffable across
time.

Usage:
  uv run python scripts/snapshot_roster.py            # writes backups/roster_<date>.csv
  # then: git add backups/ && git commit -m "..."

The output path (backups/roster_<date>.csv) is git-tracked (data/backups is NOT
-- this uses the repo-root backups/ dir).
"""
from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "backups"
DB = ROOT / "data" / "ops.sqlite"

FIELDS = ["creator_id", "creator_name", "platform", "handle", "profile_url",
          "results_type", "results_limit", "tier", "enabled", "created_at", "updated_at"]
# p.created_at doesn't exist (creators.created_at is the source); aliased below.
SELECT = (
    "SELECT c.id AS creator_id, c.name AS creator_name, c.created_at AS created_at, "
    "p.platform, p.handle, p.profile_url, p.results_type, p.results_limit, p.tier, "
    "p.enabled, p.updated_at "
    "FROM profiles p JOIN creators c ON c.id = p.creator_id ORDER BY p.handle"
)


def main() -> int:
    if not DB.exists():
        raise SystemExit(f"no {DB}")
    c = sqlite3.connect(str(DB))
    c.row_factory = sqlite3.Row
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"roster_{date}.csv"
    rows = c.execute(SELECT).fetchall()
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for p in rows:
            w.writerow({f: p[f] for f in FIELDS})
    c.close()
    print(f"wrote {len(rows)} roster rows -> {out} ({out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
