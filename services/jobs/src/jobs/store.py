"""Durable job/item store for qwen-batch-service (its own SQLite database).

Holds job lifecycle + per-item state so a crash loses at most the in-flight
item and the service resumes the rest. This database is the service's private
state — deliberately decoupled from any consumer's ops database.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

PENDING = "pending"
PROCESSING = "processing"
COMPLETED = "completed"
FAILED = "failed"

JOB_PENDING = "pending"
JOB_PROCESSING = "processing"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_MAX_TOKENS = 2500
BACKOFF_S = 8.0  # base exponential backoff for transient qwen failures
LEASE_S = 300.0  # worker lease: a `processing` claim older than this was lost and is reclaimable


log = logging.getLogger("jobs.store")


@dataclass
class Item:
    id: str
    job_id: str
    custom_key: str
    prompt: str
    images: list[str] = field(default_factory=list)
    state: str = PENDING
    output: str | None = None
    error: str | None = None
    attempts: int = 0
    scheduled_for: float = 0.0


class Store:
    """SQLite-backed store. Single-writer via a module-level lock."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    model TEXT NOT NULL,
                    max_tokens INTEGER NOT NULL DEFAULT 2500,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    total INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS items (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    custom_key TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    images_json TEXT NOT NULL DEFAULT '[]',
                    state TEXT NOT NULL,
                    output TEXT,
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    scheduled_for REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_items_job ON items(job_id);
                CREATE INDEX IF NOT EXISTS idx_items_claim
                    ON items(job_id, state, scheduled_for);
                """
            )
            conn.commit()
        finally:
            conn.close()

    # -- jobs ---------------------------------------------------------------
    def create_job(
        self,
        items: list[dict],
        *,
        model: str,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> tuple[str, int]:
        job_id = uuid.uuid4().hex
        now = time.time()
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO jobs (id,state,model,max_tokens,max_attempts,total,"
                    "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                    (job_id, JOB_PENDING, model, max_tokens, max_attempts, len(items), now, now),
                )
                for it in items:
                    conn.execute(
                        "INSERT INTO items (id,job_id,custom_key,prompt,images_json,state,"
                        "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                        (
                            uuid.uuid4().hex,
                            job_id,
                            it["custom_key"],
                            it["prompt"],
                            json.dumps(it.get("images", [])),
                            PENDING,
                            now,
                            now,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        return job_id, len(items)

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return dict(row) if row else None
            finally:
                conn.close()

    def list_jobs(self, limit: int = 100) -> list[dict]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    # -- items --------------------------------------------------------------
    def claim_item(self, job_id: str, *, now: float,
                   lease_s: float = LEASE_S) -> Item | None:
        """Atomically claim one pending, retry-eligible item from a job.

        Also enforces the worker lease: an item still `processing` whose
        claim is older than lease_s was held by a worker that died before
        resolving it (crash, kill, power loss). With attempts remaining it
        is reclaimed and retried (the reclaim counts as a fresh attempt);
        with attempts exhausted it is failed terminally so the job can
        still roll up instead of wedging in `processing` forever.
        """
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                job = self._get_job(conn, job_id)
                if job is None:
                    return None
                row = conn.execute(
                    "SELECT * FROM items WHERE job_id=? AND state=? AND scheduled_for<=? "
                    "AND attempts < ? ORDER BY created_at, rowid LIMIT 1",
                    (job_id, PENDING, now, job["max_attempts"]),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        "SELECT * FROM items WHERE job_id=? AND state=? AND updated_at<? "
                        "ORDER BY created_at, rowid LIMIT 1",
                        (job_id, PROCESSING, now - lease_s),
                    ).fetchone()
                    if row is None:
                        return None
                    if row["attempts"] >= job["max_attempts"]:
                        error = (f"worker lease expired ({int(lease_s)}s) "
                                 "with attempts exhausted")
                        log.error("item %s (%s) failed terminally: %s",
                                  row["id"], row["custom_key"], error)
                        conn.execute(
                            "UPDATE items SET state=?, error=?, updated_at=? WHERE id=?",
                            (FAILED, error, now, row["id"]),
                        )
                        conn.execute(
                            "UPDATE jobs SET failed=failed+1, updated_at=? WHERE id=?",
                            (now, job_id),
                        )
                        self._rollup_job(conn, job_id)
                        conn.commit()
                        return None
                conn.execute(
                    "UPDATE items SET state=?, attempts=attempts+1, updated_at=? "
                    "WHERE id=?",
                    (PROCESSING, now, row["id"]),
                )
                conn.commit()
                return Item(
                    id=row["id"],
                    job_id=row["job_id"],
                    custom_key=row["custom_key"],
                    prompt=row["prompt"],
                    images=json.loads(row["images_json"] or "[]"),
                    attempts=row["attempts"] + 1,
                )
            finally:
                conn.close()

    def resolve_item(self, item: Item, *, output: str | None = None,
                     error: str | None = None, backoff_s: float = BACKOFF_S,
                     terminal: bool = False) -> None:
        """Finalize a claimed item: success, terminal failure, or retry-backoff.

        A retry keeps the item pending but schedules it into the future; when
        attempts are exhausted (or the job policy says so) it becomes FAILED.
        """
        now = time.time()
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                job = self._get_job(conn, item.job_id)
                if (
                    error is not None
                    and not terminal
                    and job
                    and item.attempts < job["max_attempts"]
                ):
                    # transient -> retry later
                    conn.execute(
                        "UPDATE items SET state=?, error=?, scheduled_for=?, updated_at=? "
                        "WHERE id=?",
                        (
                            PENDING,
                            error,
                            now + backoff_s * (2 ** (item.attempts - 1)),
                            now,
                            item.id,
                        ),
                    )
                elif error is not None:
                    conn.execute(
                        "UPDATE items SET state=?, error=?, updated_at=? WHERE id=?",
                        (FAILED, error, now, item.id),
                    )
                    conn.execute(
                        "UPDATE jobs SET failed=failed+1, updated_at=? WHERE id=?",
                        (now, item.job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE items SET state=?, output=?, updated_at=? WHERE id=?",
                        (COMPLETED, output, now, item.id),
                    )
                    conn.execute(
                        "UPDATE jobs SET completed=completed+1, updated_at=? WHERE id=?",
                        (now, item.job_id),
                    )
                self._rollup_job(conn, item.job_id)
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _rollup_job(conn: sqlite3.Connection, job_id: str) -> None:
        job = Store._get_job(conn, job_id)
        if job["failed"] >= job["total"] and job["total"] > 0:
            conn.execute(
                "UPDATE jobs SET state=? WHERE id=?", (JOB_FAILED, job_id)
            )
        elif job["completed"] + job["failed"] >= job["total"]:
            conn.execute(
                "UPDATE jobs SET state=? WHERE id=?", (JOB_COMPLETED, job_id)
            )
        else:
            conn.execute(
                "UPDATE jobs SET state=? WHERE id=?", (JOB_PROCESSING, job_id)
            )

    def job_items(self, job_id: str) -> list[dict]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT custom_key,state,output,error,attempts FROM items "
                    "WHERE job_id=? ORDER BY created_at",
                    (job_id,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    @staticmethod
    def _get_job(conn: sqlite3.Connection, job_id: str) -> dict | None:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None
