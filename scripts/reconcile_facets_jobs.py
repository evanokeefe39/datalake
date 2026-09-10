#!/usr/bin/env python3
"""Reconcile ``facets_batch_jobs`` (data/ops.sqlite) against the qwen-batch-service job store.

Phase 1 (ADR-0012/0013) retires the ops.sqlite queue. Before the ledger table can
be dropped, every live row must be classified: a ledger row can represent
in-flight work, and dropping it would lose the handle to a job the service
still owns.

Default mode is a STRICTLY READ-ONLY report: it prints a classification table
and writes nothing to either database. ``--apply`` deletes ONLY rows classified
TERMINAL_AND_HARVESTED; it refuses to touch IN_FLIGHT or ABANDONED rows and it
NEVER drops the table.

Run with: uv run python scripts/reconcile_facets_jobs.py
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

OPS_DB = Path("data/ops.sqlite")
SERVICE_DB = Path.home() / ".qwen-batch" / "state.sqlite"

# Ledger statuses that mean the batch lifecycle reached a terminal point
# (nothing left for the service to do).
LEDGER_TERMINAL_STATUSES = {"RETRIEVED", "JOB_FAILED", "COMPLETED", "FAILED"}
# Service-side states that are terminal.
SERVICE_TERMINAL_STATES = {"completed", "failed"}

TERMINAL_AND_HARVESTED = "TERMINAL_AND_HARVESTED"
IN_FLIGHT = "IN_FLIGHT"
ABANDONED = "ABANDONED"
ORPHANED_HANDLE = "ORPHANED_HANDLE"


@dataclass(frozen=True)
class LedgerRow:
    id: int
    job_id: str
    status: str
    n_requests: int
    model: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ServiceJob:
    job_id: str
    state: str
    total: int
    completed: int
    failed: int
    updated_at: str  # ISO


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def open_readonly(path: Path) -> sqlite3.Connection:
    """Open a SQLite file strictly read-only (fails if the file does not exist)."""
    if not path.exists():
        raise FileNotFoundError(f"database not found: {path}")
    uri = f"file:{path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_ledger(conn: sqlite3.Connection) -> list[LedgerRow]:
    rows = conn.execute(
        "SELECT id, job_id, status, n_requests, model, created_at, updated_at"
        " FROM facets_batch_jobs ORDER BY id"
    ).fetchall()
    return [LedgerRow(*r) for r in rows]


def load_service_jobs(conn: sqlite3.Connection) -> dict[str, ServiceJob]:
    jobs: dict[str, ServiceJob] = {}
    for job_id, state, total, completed, failed, updated_at in conn.execute(
        "SELECT id, state, total, completed, failed, updated_at FROM jobs"
    ):
        jobs[job_id] = ServiceJob(job_id, state, total, completed, failed, _iso(updated_at))
    return jobs


def classify(row: LedgerRow, service: dict[str, ServiceJob]) -> tuple[str, str]:
    """Return (bucket, evidence) for one ledger row."""
    sj = service.get(row.job_id)
    if sj is None:
        if row.status in LEDGER_TERMINAL_STATUSES:
            # Terminal on the ledger side even though the service has no record
            # (e.g. service store pruned): nothing in flight remains.
            return TERMINAL_AND_HARVESTED, (
                f"ledger status={row.status} (terminal) but NO service record; "
                "no in-flight handle exists"
            )
        return ABANDONED, (
            f"ledger status={row.status} (non-terminal) and NO service record; "
            "needs explicit human decision"
        )
    if sj.state in SERVICE_TERMINAL_STATES:
        evidence = (
            f"ledger status={row.status}; service state={sj.state} "
            f"({sj.completed}/{sj.total} completed, {sj.failed} failed, "
            f"service updated {sj.updated_at})"
        )
        return TERMINAL_AND_HARVESTED, evidence
    return IN_FLIGHT, (
        f"ledger status={row.status}; service state={sj.state} "
        f"({sj.completed}/{sj.total} completed, {sj.failed} failed, "
        f"service updated {sj.updated_at}) — service still owns this job"
    )


def find_orphans(ledger: Sequence[LedgerRow], service: dict[str, ServiceJob]) -> list[ServiceJob]:
    known = {r.job_id for r in ledger}
    return [sj for jid, sj in sorted(service.items()) if jid not in known]


def build_report() -> tuple[list[tuple], list[tuple[str, str]], int, int, int]:
    ledger_conn = open_readonly(OPS_DB)
    try:
        ledger = load_ledger(ledger_conn)
    finally:
        ledger_conn.close()
    service_conn = open_readonly(SERVICE_DB)
    try:
        service = load_service_jobs(service_conn)
    finally:
        service_conn.close()

    classifications: list[tuple[LedgerRow, str, str]] = []
    safe = in_flight = abandoned = 0
    for row in ledger:
        bucket, evidence = classify(row, service)
        classifications.append((row, bucket, evidence))
        if bucket == TERMINAL_AND_HARVESTED:
            safe += 1
        elif bucket == IN_FLIGHT:
            in_flight += 1
        else:
            abandoned += 1
    orphans = find_orphans(ledger, service)
    orphan_tuples = [
        (sj.job_id, sj.state, f"{sj.completed}/{sj.total} completed, updated {sj.updated_at}")
        for sj in orphans
    ]
    summary = [(r.id, r.job_id, r.status, b, e) for (r, b, e) in classifications]
    return summary, orphan_tuples, safe, in_flight, abandoned


def print_report() -> int:
    rows, orphans, safe, in_flight, abandoned = build_report()
    print(f"facets_batch_jobs rows: {len(rows)}")
    print()
    print(f"{'id':>3}  {'job_id':<34} {'status':<12} {'bucket':<24} evidence")
    for rid, job_id, status, bucket, evidence in rows:
        print(f"{rid:>3}  {job_id:<34} {status:<12} {bucket:<24} {evidence}")
    print()
    print(f"ORPHANED service jobs (no ledger row): {len(orphans)}")
    for job_id, state, detail in orphans:
        print(f"     {job_id}  state={state:<10} {detail}")
    print()
    print(f"safe to drop (TERMINAL_AND_HARVESTED): {safe}")
    print(f"IN_FLIGHT (blocks drop): {in_flight}")
    print(f"ABANDONED (blocks drop): {abandoned}")
    blockers = in_flight + abandoned
    print()
    if blockers:
        print(f"RESULT: {blockers} row(s) BLOCK the drop of facets_batch_jobs.")
    else:
        print("RESULT: all rows terminal; facets_batch_jobs is safe to drop (in a later step).")
    return 0


def apply_reconciliation(dry_run_assert: bool = False) -> int:
    """Delete ONLY TERMINAL_AND_HARVESTED ledger rows. Never drops the table."""
    if OPS_DB.resolve() != Path("data/ops.sqlite").resolve():
        print(f"refusing: unexpected ops db path {OPS_DB}", file=sys.stderr)
        return 2
    rows, orphans, safe, in_flight, abandoned = build_report()
    blockers = [r for r in rows if r[3] in (IN_FLIGHT, ABANDONED)]
    terminal_ids = [r[0] for r in rows if r[3] == TERMINAL_AND_HARVESTED]
    print(f"would delete {len(terminal_ids)} TERMINAL_AND_HARVESTED row(s): {terminal_ids}")
    if blockers:
        print(
            f"REFUSING --apply: {len(blockers)} row(s) are IN_FLIGHT/ABANDONED "
            "and block reconciliation. Resolve them first (human decision).",
            file=sys.stderr,
        )
        for rid, job_id, status, bucket, _ in blockers:
            print(
                f"  blocked: id={rid} job={job_id} status={status} bucket={bucket}",
                file=sys.stderr,
            )
        return 1
    if not terminal_ids:
        print("nothing to do.")
        return 0
    conn = sqlite3.connect(OPS_DB.as_posix())  # read-write, ledger only
    try:
        placeholders = ",".join("?" for _ in terminal_ids)
        cur = conn.execute(
            f"DELETE FROM facets_batch_jobs WHERE id IN ({placeholders})", terminal_ids
        )
        conn.commit()
        print(f"deleted {cur.rowcount} row(s) from facets_batch_jobs (table preserved).")
    finally:
        conn.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="delete TERMINAL_AND_HARVESTED rows (default: read-only report)",
    )
    args = parser.parse_args(argv)
    if args.apply:
        return apply_reconciliation()
    return print_report()


if __name__ == "__main__":
    sys.exit(main())
