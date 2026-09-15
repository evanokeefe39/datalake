"""W9 retirement: archive every doomed table, verify the archive, then drop.

The tables dropped here are IRREVERSIBLE. The owner authorized the queue drops
(2026-09-14: "i dont care about the queues and batches in ops.sqlite we can
confidently drop them") but the sequence is not negotiable:

    archive -> verify (export count == live count, SAME RUN) -> drop

An unchecked export is not a copy, and a copy that was never counted is not
safe. The gate is deliberately SELF-REFERENTIAL -- export count is compared to
the live count measured in this same run, never against a frozen constant. A
frozen `== 9,576` would fail on any legitimate drift (a manual trigger, a
restored job, a backfilled row) and misreport it as an archive error. The
2026-09-14 baseline (gold_analyses 9,576 / dead_letter 776 / gold_growth_facets
205 / facets_batch_jobs 4) is therefore a DRIFT SIGNAL, not a gate -- a live
count that differs means something wrote the table after W-FREEZE, which is a
W-FREEZE regression to investigate BEFORE archiving, not during.

Modes
-----
  --plan       (default) report what would be archived/dropped. Writes nothing.
  --rehearse   run the FULL archive+verify path against a COPY of the databases
               in a temp dir, then drop there. Proves the script end-to-end
               without touching live data. This is the acceptance evidence.
  --apply      archive + verify + drop against the LIVE databases.
               Requires the §3.0 backup gate to have passed AND human approval.

Never drops the database file itself -- per-table drops only.

Usage:
  uv run python scripts/retire_queue_tables.py --plan
  uv run python scripts/retire_queue_tables.py --rehearse
  uv run python scripts/retire_queue_tables.py --apply --i-have-approval

Notes: run when no Dagster daemon/dashboard holds state.duckdb -- it is
single-writer and a concurrent handle makes the write fail or interleave
(ADR-0012 decision 8 records the observed "Cannot open file ... being used by
another process").
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
OPS_DB = ROOT / "data" / "ops.sqlite"
STATE_DB = ROOT / "data" / "state.duckdb"
ARCHIVE_ROOT = ROOT / "data" / "lake" / "archive"

# (database, table). Order is cosmetic; every table is archived before any drop.
DOOMED: tuple[tuple[str, str], ...] = (
    ("ops", "batch_jobs"),
    ("ops", "batch_items"),
    ("ops", "dead_letter"),
    ("ops", "facets_batch_jobs"),
    ("ops", "media_metadata"),
    ("state", "gold_growth_facets"),
    ("state", "gold_analyses"),
)

# The KEEP set must survive intact and non-empty. Asserted after the drops --
# a retirement that also removed these would be catastrophic, and the plan
# names them explicitly as retained.
KEEP_ALIVE: tuple[tuple[str, str], ...] = (
    ("ops", "media_cache"),
    ("ops", "creators"),
    ("ops", "profiles"),
    ("ops", "creator_merges"),
    ("ops", "prompt_registry"),
)

# Dated drift signal, NOT a gate (see module docstring).
BASELINE_2026_09_14 = {
    "gold_analyses": 9576,
    "dead_letter": 776,
    "gold_growth_facets": 205,
    "facets_batch_jobs": 4,
}


class RetirementError(RuntimeError):
    """Loud, never swallowed: the retirement refuses to report success."""


def _log(msg: str) -> None:
    print(msg, flush=True)


# ── counting ────────────────────────────────────────────────────────────────


def _sqlite_tables(con: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _duckdb_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }


def _count_ops(con: sqlite3.Connection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _count_state(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ── archive + verify ────────────────────────────────────────────────────────


def archive_and_verify(
    ops: sqlite3.Connection,
    state: duckdb.DuckDBPyConnection,
    archive_root: Path,
    now: str,
) -> list[dict]:
    """Export every doomed table, then verify export count == live count.

    Both counts are measured in THIS run. A mismatch raises -- it never
    warns-and-continues, because continuing would drop unaudited data.
    """
    manifest: list[dict] = []
    for db_name, table in DOOMED:
        con = ops if db_name == "ops" else state
        tables = _sqlite_tables(ops) if db_name == "ops" else _duckdb_tables(state)
        if table not in tables:
            # Already gone (idempotent re-run) -- record it, do not fail.
            _log(f"  {table:22} ABSENT (already retired)")
            manifest.append(
                {"db": db_name, "table": table, "live": 0, "exported": 0,
                 "export": None, "status": "absent"}
            )
            continue

        live = (
            _count_ops(ops, table) if db_name == "ops"
            else _count_state(state, table)
        )
        out_dir = archive_root / table / now
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{table}.parquet"

        if db_name == "ops":
            # SQLite -> Arrow -> Parquet, so the export does not depend on any
            # DuckDB sqlite extension being installed.
            import polars as pl

            cur = con.execute(f"SELECT * FROM {table}")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            pl.DataFrame(rows, infer_schema_length=None).write_parquet(out)
        else:
            con.execute(
                f"COPY {table} TO '{out.as_posix()}' (FORMAT PARQUET)"
            )

        # Verify against the SAME RUN's live count, measured above.
        exported = _verify_export(out, db_name)
        status = "ok" if exported == live else "MISMATCH"
        _log(
            f"  {table:22} live={live:<7} exported={exported:<7} {status}"
        )
        manifest.append(
            {"db": db_name, "table": table, "live": live,
             "exported": exported, "export": str(out), "status": status}
        )
        if exported != live:
            raise RetirementError(
                f"ARCHIVE VERIFY FAILED for {table}: export count {exported} "
                f"!= live count {live}. Refusing to drop -- an unverified "
                "archive is not a copy."
            )
    return manifest


def _verify_export(path: Path, db_name: str) -> int:
    """Row count of the written Parquet, read back from disk."""
    import polars as pl

    if not path.exists():
        raise RetirementError(f"archive file was not written: {path}")
    return pl.scan_parquet(path).select(pl.len()).collect().item()


# ── drops ───────────────────────────────────────────────────────────────────


def drop_doomed(ops: sqlite3.Connection,
                state: duckdb.DuckDBPyConnection) -> list[str]:
    """Per-table drops. NEVER the database file."""
    dropped: list[str] = []
    for db_name, table in DOOMED:
        if db_name == "ops":
            if table not in _sqlite_tables(ops):
                continue
            ops.execute(f"DROP TABLE {table}")
            dropped.append(f"ops.{table}")
        else:
            if table not in _duckdb_tables(state):
                continue
            # Views over the table must go first -- DuckDB refuses a drop with
            # dependent views, and silently leaving a broken view is worse.
            state.execute(f"DROP TABLE {table} CASCADE")
            dropped.append(f"state.{table}")
    ops.commit()
    return dropped


def reconcile_facets_jobs(before_drop: Path | None) -> list[dict]:
    """Resolve every ledger job id against the SERVICE's own job store.

    W9's plan precondition (remediation-plan.md:473-478): the 4
    `facets_batch_jobs` rows must be reconciled into the service's job store
    BEFORE the drop. Job ids are minted service-side, so this ledger is the
    only LOCAL index of which service jobs matter -- after the drop nothing
    on this host names them. A Parquet archive nobody reads is not a pointer,
    so the mapping is written durably (W9 log) as part of --apply.

    Reads the service store read-only; never writes it.
    """
    if before_drop is None or not before_drop.exists():
        return []
    import sqlite3 as _sq

    ledger = _sq.connect(f"file:{before_drop}?mode=ro", uri=True)
    try:
        rows = ledger.execute(
            "SELECT job_id, mode, status, n_requests FROM facets_batch_jobs"
        ).fetchall()
    except _sq.OperationalError:
        # Table already gone (re-run after a successful retirement).
        return []
    finally:
        pass

    store_path = Path.home() / ".qwen-batch" / "state.sqlite"
    out: list[dict] = []
    if not store_path.exists():
        for job_id, mode, status, n in rows:
            out.append({
                "job_id": job_id, "mode": mode, "ledger_status": status,
                "n_requests": n, "service_state": "STORE-UNAVAILABLE",
                "completed": None, "failed": None, "pending": None,
                "disposition": "UNRECONCILED -- service store not found; "
                               "resolve manually before relying on this id",
            })
        ledger.close()
        return out

    store = _sq.connect(f"file:{store_path}?mode=ro", uri=True)
    for job_id, mode, status, n in rows:
        rec = store.execute(
            "SELECT state, completed, failed, total FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if rec is None:
            out.append({
                "job_id": job_id, "mode": mode, "ledger_status": status,
                "n_requests": n, "service_state": "ABSENT",
                "completed": None, "failed": None, "pending": None,
                "disposition": "not in the service store -- expired or pruned; "
                               "no action possible",
            })
            continue
        state, completed, failed, total = rec
        pending = (total or 0) - (completed or 0) - (failed or 0)
        if state in ("completed", "failed"):
            disposition = "terminal -- no action"
        else:
            disposition = (
                f"OPEN: {pending} item(s) outstanding. Decide harvest-continue "
                "/ harvest-abandon / declare-dead BEFORE dropping this table"
            )
        out.append({
            "job_id": job_id, "mode": mode, "ledger_status": status,
            "n_requests": n, "service_state": state,
            "completed": completed, "failed": failed, "pending": pending,
            "disposition": disposition,
        })
    store.close()
    ledger.close()
    return out


def assert_keep_set(ops: sqlite3.Connection,
                    state: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """The KEEP set must be present AND non-empty after the drops."""
    counts: dict[str, int] = {}
    for db_name, table in KEEP_ALIVE:
        tables = _sqlite_tables(ops) if db_name == "ops" else _duckdb_tables(state)
        if table not in tables:
            raise RetirementError(
                f"KEEP-SET VIOLATION: {db_name}.{table} is MISSING after the "
                "drops. It is on the retained list and must survive."
            )
        n = _count_ops(ops, table) if db_name == "ops" else _count_state(state, table)
        counts[f"{db_name}.{table}"] = n
        if n == 0:
            raise RetirementError(
                f"KEEP-SET VIOLATION: {db_name}.{table} is EMPTY after the "
                "drops -- it was retained and must be non-empty."
            )
    return counts


# ── modes ───────────────────────────────────────────────────────────────────


def _open_live() -> tuple[sqlite3.Connection, duckdb.DuckDBPyConnection]:
    ops = sqlite3.connect(str(OPS_DB))
    state = duckdb.connect(str(STATE_DB))
    return ops, state


def _open_rehearsal() -> tuple[sqlite3.Connection, duckdb.DuckDBPyConnection,
                               Path]:
    """Copies of both DBs in a temp dir -- the full path, zero live risk."""
    tmp = Path(tempfile.mkdtemp(prefix="retire-rehearsal-"))
    ops_copy = tmp / "ops.sqlite"
    state_copy = tmp / "state.duckdb"
    src_ops = sqlite3.connect(str(OPS_DB))
    dst_ops = sqlite3.connect(str(ops_copy))
    src_ops.backup(dst_ops)  # online-backup API: safe even if open
    dst_ops.close()
    src_ops.close()
    shutil.copy2(STATE_DB, state_copy)
    return (
        sqlite3.connect(str(ops_copy)),
        duckdb.connect(str(state_copy)),
        tmp,
    )


def run(apply_: bool, rehearse: bool) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not apply_ and not rehearse:
        # --plan: measure, report, write nothing.
        ops, state = _open_live()
        try:
            _log("PLAN (writes nothing)")
            _log(f"{'table':24} {'live':>8}  {'baseline':>9}  drift")
            total = 0
            for db_name, table in DOOMED:
                tables = (
                    _sqlite_tables(ops) if db_name == "ops"
                    else _duckdb_tables(state)
                )
                if table not in tables:
                    _log(f"{table:24} {'ABSENT':>8}")
                    continue
                n = (
                    _count_ops(ops, table) if db_name == "ops"
                    else _count_state(state, table)
                )
                total += n
                base = BASELINE_2026_09_14.get(table)
                drift = "" if base is None else (
                    "same" if n == base else f"DRIFT {n - base:+d}"
                )
                _log(f"{table:24} {n:>8}  {str(base or '-'):>9}  {drift}")
            _log(f"\ntotal rows to archive: {total}")
            _log("KEEP set (must survive):")
            for db_name, table in KEEP_ALIVE:
                tables = (
                    _sqlite_tables(ops) if db_name == "ops"
                    else _duckdb_tables(state)
                )
                if table not in tables:
                    _log(f"  {table:22} MISSING")
                    continue
                n = (
                    _count_ops(ops, table) if db_name == "ops"
                    else _count_state(state, table)
                )
                _log(f"  {table:22} {n}")
            return {"mode": "plan", "rows": total}
        finally:
            ops.close()
            state.close()

    if rehearse:
        ops, state, tmp = _open_rehearsal()
        archive_root = tmp / "archive"
        _log(f"REHEARSAL on copies in {tmp} (live data untouched)")
    else:
        ops, state = _open_live()
        archive_root = ARCHIVE_ROOT
        _log("APPLY against LIVE databases")

    try:
        _log("\n1. reconcile the facets_batch_jobs handle index BEFORE any drop")
        recon = reconcile_facets_jobs(OPS_DB)
        if not recon:
            _log("  (no facets_batch_jobs rows to reconcile)")
        for r in recon:
            _log(
                f"  {r['job_id'][:12]}…  ledger={r['ledger_status']:10} "
                f"service={r['service_state']:18} pending={r['pending']}  "
                f"{r['disposition']}"
            )
        open_handles = [
            r for r in recon
            if r["service_state"] not in ("completed", "failed", "ABSENT")
        ]
        if open_handles:
            _log(
                f"\n  !! {len(open_handles)} job(s) have NO terminal state in the "
                "service store. After this drop nothing on this host names them."
            )
            for r in open_handles:
                _log(f"     {r['job_id']}  -> {r['disposition']}")

        _log("\n2. archive + verify (export count == live count, same run)")
        manifest = archive_and_verify(ops, state, archive_root, now)

        _log("\n3. drop (per-table only -- never the database file)")
        dropped = drop_doomed(ops, state)
        for d in dropped:
            _log(f"  dropped {d}")
        if not dropped:
            _log("  (nothing to drop)")

        _log("\n4. KEEP-set assertion (must survive, non-empty)")
        keep = assert_keep_set(ops, state)
        for k, v in keep.items():
            _log(f"  {k:22} {v}")

        out = {
            "mode": "rehearse" if rehearse else "apply",
            "at": now,
            "reconciliation": recon,
            "open_handles": open_handles,
            "manifest": manifest,
            "dropped": dropped,
            "keep_set": keep,
        }
        if not rehearse:
            logdir = ROOT / "data" / "logs"
            logdir.mkdir(parents=True, exist_ok=True)
            log = logdir / f"w9-retirement-{now}.json"
            log.write_text(json.dumps(out, indent=2), encoding="utf-8")
            _log(f"\nlog: {log}")
        _log("\nRETIREMENT OK")
        return out
    finally:
        ops.close()
        state.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--plan", action="store_true", default=False)
    g.add_argument("--rehearse", action="store_true", default=False)
    g.add_argument("--apply", action="store_true", default=False)
    p.add_argument(
        "--i-have-approval", action="store_true", default=False,
        help="required with --apply: records that human approval was given "
             "(the plan gates every DROP against live data on it)",
    )
    a = p.parse_args()

    if a.apply and not a.i_have_approval:
        raise SystemExit(
            "REFUSING: --apply drops live tables and the remediation plan "
            "gates that on explicit human approval. Pass --i-have-approval "
            "only after the §3.0 backup gate has passed and the owner has "
            "approved the drop list."
        )
    run(apply_=a.apply, rehearse=a.rehearse)


if __name__ == "__main__":
    main()
