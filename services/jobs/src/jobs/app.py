"""FastAPI app for qwen-batch-service.

Endpoints:
- POST /jobs                submit a job {items:[{custom_key,prompt,images}]}, model?, max_tokens?
- GET  /jobs                list recent jobs
- GET  /jobs/{job_id}       poll state {state,total,completed,failed,error}
- GET  /jobs/{job_id}/results  harvest {items:[{custom_key,ok,output,error}]}
- GET  /health              liveness + config
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import __version__
from .store import DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_TOKENS, Store
from .worker import Worker

log = logging.getLogger("jobs.app")

DEFAULT_MODEL = "qwen/qwen3.7-flash"


def _db_path() -> str:
    return os.environ.get("QWEN_BATCH_DB") or str(
        Path.home() / ".qwen-batch" / "state.sqlite"
    )


def _model() -> str:
    return os.environ.get("OPENROUTER_MODEL") or DEFAULT_MODEL


store = Store(_db_path())
worker = Worker(store)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.environ.get("QWEN_BATCH_DISABLE_WORKER") != "1":
        worker.start()
    yield
    worker.stop()


app = FastAPI(title="qwen-batch-service", version=__version__, lifespan=lifespan)


# -- request/response models -------------------------------------------------

class ItemIn(BaseModel):
    custom_key: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    images: list[str] = Field(default_factory=list)


class JobIn(BaseModel):
    items: list[ItemIn] = Field(min_length=1)
    model: str | None = None
    max_tokens: int | None = None


# -- endpoints ---------------------------------------------------------------

@app.post("/jobs")
def submit_job(body: JobIn) -> dict:
    job_id, total = store.create_job(
        [i.model_dump() for i in body.items],
        model=body.model or _model(),
        max_tokens=body.max_tokens or DEFAULT_MAX_TOKENS,
        max_attempts=DEFAULT_MAX_ATTEMPTS,
    )
    log.info("job %s submitted (%d items)", job_id, total)
    return {"job_id": job_id, "total": total}


@app.get("/jobs")
def list_jobs() -> dict:
    return {"jobs": store.list_jobs()}


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return {
        "job_id": job["id"],
        "state": job["state"],
        "total": job["total"],
        "completed": job["completed"],
        "failed": job["failed"],
        "error": job["error"],
    }


@app.get("/jobs/{job_id}/results")
def get_results(job_id: str) -> dict:
    if store.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    items = [
        {"custom_key": it["custom_key"], "ok": it["state"] == "completed",
         "output": it["output"], "error": it["error"]}
        for it in store.job_items(job_id)
    ]
    return {"items": items}


@app.get("/health")
def health() -> dict:
    """Readiness gate: 200 only when the store is readable AND the worker runs.

    Every consumer of this endpoint (the submit gate, the harvest gate, the
    sensor's provider check, the adapter's `require_health`) treats a non-200 as
    "the service is unusable" — so this must actually mean that. A hardcoded
    `{"status": "ok"}` reports a service whose worker has died, or whose store
    is unreadable, as ready; submissions then queue forever and the platform
    looks idle rather than broken.
    """
    reasons: list[str] = []

    try:
        conn = sqlite3.connect(f"file:{store.db_path}?mode=ro", uri=True)
        try:
            conn.execute("SELECT 1 FROM jobs LIMIT 1")
        finally:
            conn.close()
    except sqlite3.Error as exc:
        # Narrow on purpose: only a genuine sqlite failure means "store
        # unreadable". A NameError/AttributeError here is a BUG in this
        # function, and swallowing it would report a healthy service as down
        # (or worse, a broken one as up) with a misleading reason.
        reasons.append(f"store unreadable at {store.db_path}: {exc}")

    if not worker.alive():
        reasons.append("worker thread is not running")

    if reasons:
        raise HTTPException(503, "; ".join(reasons))

    return {"status": "ok", "model": _model(), "version": __version__}
