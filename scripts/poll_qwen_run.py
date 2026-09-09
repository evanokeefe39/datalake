#!/usr/bin/env python
"""Poll progress of the running qwen-batch visual/text corpus job.

Reads the qwen-batch *service* job store (the authoritative source the driver
polls) and prints state / completion / failure / rate / ETA. Read-only — safe
to run while a driver is running, and useful to confirm whether a run reached
terminal (then re-run the driver to harvest + continue).

Usage:
    .venv/Scripts/python.exe scripts/poll_qwen_run.py [--db <service.sqlite>]
                                                      [--recent N] [--failures N]

DB default matches the service's own default (QWEN_BATCH_DB env, else
~/.qwen-batch/state.sqlite). Exit code 0 = a job exists; 2 = no service DB /
no jobs found.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path


def _default_db() -> Path:
    env = os.environ.get("QWEN_BATCH_DB")
    return Path(env) if env else Path.home() / ".qwen-batch" / "state.sqlite"


def _connect(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"service DB not found: {db_path}")
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def _fmt_eta(seconds: float) -> str:
    h = seconds / 3600
    if h >= 24:
        return f"{h / 24:.1f}d"
    return f"{h:.1f}h"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=None,
                    help="service sqlite path (default: QWEN_BATCH_DB or "
                         "~/.qwen-batch/state.sqlite)")
    ap.add_argument("--recent", type=int, default=3, help="how many recent jobs to show")
    ap.add_argument("--failures", type=int, default=5,
                    help="how many recent failed items to show (0 disables)")
    args = ap.parse_args()

    db_path = Path(args.db) if args.db else _default_db()
    try:
        con = _connect(db_path)
    except FileNotFoundError as e:
        print(f"[poll] {e}")
        return 2

    jobs = con.execute(
        "SELECT id, state, model, total, completed, failed, created_at, updated_at "
        "FROM jobs ORDER BY created_at DESC LIMIT ?",
        (args.recent,),
    ).fetchall()
    if not jobs:
        print("[poll] no jobs in the service store yet — driver may still be discovering.")
        return 2

    now = time.time()
    newest = jobs[0]
    for j in jobs:
        d = dict(j)
        total, done = d["total"], d["completed"]
        pct = 100.0 * done / total if total else 0.0
        elapsed = (now - d["created_at"]) / 60
        rate = done / elapsed if elapsed > 0 else 0.0
        eta_s = f"~{_fmt_eta((total - done) / rate * 60.0)}" if rate > 0 else "n/a"
        line = (
            f"[poll] {d['id'][:12]} {d['state']:<11} {d['model']:<22} "
            f"{done}/{total} ({pct:.2f}%) failed={d['failed']}"
        )
        if d["state"] in ("processing", "pending", "submitted"):
            line += f" | {rate:.1f}/min | ETA {eta_s}"
        print(line)

    if args.failures > 0:
        fails = con.execute(
            "SELECT custom_key, error FROM items WHERE state='failed' "
            "AND job_id = ? ORDER BY updated_at DESC LIMIT ?",
            (newest["id"], args.failures),
        ).fetchall()
        if fails:
            print(f"\n[poll] recent failed items in the newest job ({len(fails)} shown):")
            for f in fails:
                err = (f["error"] or "").replace("\n", " ")
                print(f"  - post {f['custom_key']}: {err[:180]}")

    print()
    if newest["state"] in ("completed", "failed", "cancelled"):
        print("[poll] Newest job reached a terminal state — "
              "re-run the driver to harvest + continue:")
        print(
            "    .venv/Scripts/python.exe scripts/enrich_facets_batch.py "
            "--run --mode visual --service-url http://127.0.0.1:8462"
        )
    else:
        print("[poll] job(s) still in flight — no action needed yet.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
