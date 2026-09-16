"""Background drain loop for qwen-batch-service.

Runs as a daemon thread. Each tick: pick an in-flight job, claim one item,
call OpenRouter, resolve it (success / retry with backoff / terminal failure).
The API server never waits on the worker.
"""
from __future__ import annotations

import logging
import os
import threading
import time

from . import qwen
from .store import BACKOFF_S, DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_TOKENS, Store

log = logging.getLogger("jobs.worker")

POLL_IDLE_S = 1.0


class Worker:
    def __init__(self, store: Store, *, poll_interval_s: float = POLL_IDLE_S):
        self.store = store
        self.poll_interval_s = poll_interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="qwen-batch-worker", daemon=True)
        self._thread.start()
        log.info("worker started")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        log.info("worker stopped")

    def alive(self) -> bool:
        """True when the worker thread is running.

        The readiness gate needs this: a service whose worker has died accepts
        submissions and never processes them, which is indistinguishable from
        success to every caller downstream.
        """
        return self._thread is not None and self._thread.is_alive()

    def _inflight_jobs(self) -> list[dict]:
        return [
            j
            for j in self.store.list_jobs(limit=1000)
            if j["state"] in ("pending", "processing")
        ]

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                did_work = self._tick()
            except Exception:
                log.exception("worker tick failed")
                did_work = False
            if not did_work:
                self._stop.wait(self.poll_interval_s)

    def _tick(self) -> bool:
        """Claim and process one item across all in-flight jobs. True if any work done."""
        for job in self._inflight_jobs():
            item = self.store.claim_item(job["id"], now=time.time())
            if item is None:
                continue
            try:
                self._process(job, item)
            except Exception as exc:
                # An unexpected bug must never wedge the item in `processing`:
                # resolve it terminally so the job can keep making progress.
                error = f"internal error: {exc}"
                log.exception("item %s (%s) failed terminally: %s",
                              item.id, item.custom_key, error)
                self.store.resolve_item(item, error=error, terminal=True)
            return True
        return False

    def _process(self, job: dict, item) -> None:
        missing = self._missing_images(item)
        if missing:
            error = "image file(s) not found on the service host: " + ", ".join(missing)
            log.error("item %s (%s) failed terminally: %s", item.id, item.custom_key, error)
            self.store.resolve_item(item, error=error, terminal=True)
            return
        model = job["model"]
        max_tokens = job.get("max_tokens") or DEFAULT_MAX_TOKENS
        max_attempts = job.get("max_attempts") or DEFAULT_MAX_ATTEMPTS
        try:
            output = qwen.chat(item.prompt, item.images, model=model, max_tokens=max_tokens)
        except qwen.QwenError as exc:
            error = str(exc)
            if isinstance(exc, qwen.TerminalQwenError):
                log.error("item %s (%s) failed terminally: %s", item.id, item.custom_key, error)
                self.store.resolve_item(item, error=error, terminal=True)
            elif item.attempts < max_attempts:
                log.warning("item %s (%s) attempt %d/%d failed, backing off: %s",
                            item.id, item.custom_key, item.attempts, max_attempts, error)
                self.store.resolve_item(item, error=error, backoff_s=BACKOFF_S)
            else:
                log.error("item %s (%s) failed terminally: %s", item.id, item.custom_key, error)
                self.store.resolve_item(item, error=error)
        else:
            log.info("item %s (%s) completed", item.id, item.custom_key)
            self.store.resolve_item(item, output=output)


    @staticmethod
    def _missing_images(item) -> list[str]:
        return [p for p in item.images if not os.path.isfile(p)]
