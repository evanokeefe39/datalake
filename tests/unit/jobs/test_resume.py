"""US-EENG-1 AC6 durability tests: mid-run service restart and OpenRouter outage.

Covers:
- A mid-run "service restart" (fresh Store + Worker on the same SQLite file)
  never reprocesses already-COMPLETED items and finishes the remaining work.
- A transient OpenRouter outage (QwenError) retries with backoff and
  recovers once the outage ends — the job completes rather than failing.
- A persistent outage terminal-fails affected items once attempts are
  exhausted; the job rolls up to FAILED instead of wedging in processing.
- A restart after an outage-driven failure keeps completed items completed
  (never reprocessed) and failed items failed.

Determinism: jobs.store.time.time (used by resolve_item for backoff
scheduling) and claim_item's `now=` are driven by one controllable clock, so
transient retries advance without sleeping.
"""
from __future__ import annotations

from collections.abc import Iterator

import pytest
from jobs import store as store_mod
from jobs import worker as worker_mod
from jobs.qwen import QwenError
from jobs.store import (
    COMPLETED,
    FAILED,
    JOB_COMPLETED,
    JOB_FAILED,
    Store,
)
from jobs.worker import Worker


class FakeClock:
    """Single controllable clock shared by the store scheduler and claims."""

    def __init__(self) -> None:
        self._now = 0.0

    def time(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clk = FakeClock()
    monkeypatch.setattr(store_mod.time, "time", clk.time)
    monkeypatch.setattr(worker_mod.time, "time", clk.time)
    return clk


@pytest.fixture()
def db_path(tmp_path) -> str:
    return str(tmp_path / "state.sqlite")


def _make_store(db_path: str) -> Store:
    return Store(db_path)


def _make_worker(store: Store) -> Worker:
    return Worker(store)


def _job(store: Store, keys: list[str], images: list[str],
         *, max_attempts: int = 5) -> str:
    job_id, _ = store.create_job(
        [{"custom_key": k, "prompt": f"p-{k}", "images": images} for k in keys],
        model="test-model",
        max_attempts=max_attempts,
    )
    return job_id


def _chat_stub(calls: dict[str, int],
               responses: dict[str, Iterator[object]]):
    """qwen.chat replacement: counts calls per item and yields the next
    scripted response. Items are identified by their prompt ("p-<key>")."""

    def chat(prompt: str, images: list[str], *, model: str,
             max_tokens: int) -> str:
        key = prompt.removeprefix("p-")
        calls[key] = calls.get(key, 0) + 1
        outcome = next(responses[key])
        if isinstance(outcome, Exception):
            raise outcome
        return str(outcome)

    return chat


def _drive(store: Store, worker: Worker, job_id: str, clock: FakeClock,
           max_loops: int = 200) -> None:
    """Claim/process items until the job settles, advancing the clock past
    each item's backoff schedule so retries become claimable."""
    for _ in range(max_loops):
        item = store.claim_item(job_id, now=clock.time())
        if item is None:
            # Nothing claimable: skip past any pending backoff window.
            clock.advance(60.0)
            pending = [
                it for it in store.job_items(job_id) if it["state"] == "pending"
            ]
            if not pending:
                return
            continue
        job = store.get_job(job_id)
        assert job is not None
        worker._process(job, item)


def _item_states(store: Store, job_id: str) -> dict[str, dict]:
    return {it["custom_key"]: it for it in store.job_items(job_id)}


def test_service_restart_does_not_reprocess_completed(
    db_path: str, clock: FakeClock, monkeypatch: pytest.MonkeyPatch,
    tmp_path
):
    # Real image files so _missing_images does not terminal-fail the items.
    img_a = tmp_path / "a.png"
    img_b = tmp_path / "b.png"
    img_a.write_bytes(b"img")
    img_b.write_bytes(b"img")

    calls: dict[str, int] = {}
    responses: dict[str, Iterator[object]] = {
        "A": iter(["out-A"]),
        "B": iter(["out-B"]),
    }
    monkeypatch.setattr(
        worker_mod.qwen, "chat", _chat_stub(calls, responses)
    )

    store = _make_store(db_path)
    worker = _make_worker(store)
    job_id = _job(store, ["A", "B"], [str(img_a)])

    # Process only item A, then "crash" before B runs.
    item = store.claim_item(job_id, now=clock.time())
    assert item is not None and item.custom_key == "A"
    worker._process(store.get_job(job_id), item)
    assert _item_states(store, job_id)["A"]["state"] == COMPLETED
    assert calls == {"A": 1}

    # Restart: a fresh Store + Worker on the SAME database file.
    store2 = _make_store(db_path)
    worker2 = _make_worker(store2)
    _drive(store2, worker2, job_id, clock)

    states = _item_states(store2, job_id)
    assert states["A"]["state"] == COMPLETED
    assert states["B"]["state"] == COMPLETED
    assert calls["A"] == 1, "completed item A was reprocessed after restart"
    assert calls["B"] == 1
    assert store2.get_job(job_id)["state"] == JOB_COMPLETED


def test_transient_outage_retries_then_recovers(
    db_path: str, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
):
    outage = QwenError("502 upstream unavailable")
    calls: dict[str, int] = {}
    responses: dict[str, Iterator[object]] = {
        "A": iter([outage, outage, "recovered"]),
    }
    monkeypatch.setattr(
        worker_mod.qwen, "chat", _chat_stub(calls, responses)
    )

    store = _make_store(db_path)
    worker = _make_worker(store)
    job_id = _job(store, ["A"], [], max_attempts=5)

    _drive(store, worker, job_id, clock)

    states = _item_states(store, job_id)
    assert states["A"]["state"] == COMPLETED
    assert states["A"]["attempts"] == 3
    assert calls["A"] == 3
    job = store.get_job(job_id)
    assert job["state"] == JOB_COMPLETED
    assert job["completed"] == 1
    assert job["failed"] == 0


def test_persistent_outage_terminal_fails_no_wedge(
    db_path: str, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
):
    outage = QwenError("402 credits exhausted")
    calls: dict[str, int] = {}
    responses: dict[str, Iterator[object]] = {
        "A": iter([outage] * 5),
    }
    monkeypatch.setattr(
        worker_mod.qwen, "chat", _chat_stub(calls, responses)
    )

    store = _make_store(db_path)
    worker = _make_worker(store)
    job_id = _job(store, ["A"], [], max_attempts=3)

    _drive(store, worker, job_id, clock)

    states = _item_states(store, job_id)
    assert states["A"]["state"] == FAILED
    assert states["A"]["attempts"] == 3
    assert calls["A"] == 3, "no calls beyond max_attempts"
    job = store.get_job(job_id)
    assert job["state"] == JOB_FAILED
    assert job["failed"] == 1
    assert job["completed"] == 0


def test_restart_after_outage_failure_keeps_completed_and_fails_only_unfinished(
    db_path: str, clock: FakeClock, monkeypatch: pytest.MonkeyPatch,
    tmp_path
):
    img = tmp_path / "img.png"
    img.write_bytes(b"img")

    outage = QwenError("503 service unavailable")
    calls: dict[str, int] = {}
    responses: dict[str, Iterator[object]] = {
        "A": iter(["out-A"]),
        "B": iter([outage] * 5),
    }
    monkeypatch.setattr(
        worker_mod.qwen, "chat", _chat_stub(calls, responses)
    )

    store = _make_store(db_path)
    worker = _make_worker(store)
    job_id = _job(store, ["A", "B"], [str(img)], max_attempts=3)

    _drive(store, worker, job_id, clock)

    assert _item_states(store, job_id)["A"]["state"] == COMPLETED
    assert _item_states(store, job_id)["B"]["state"] == FAILED
    calls_before = dict(calls)
    assert calls_before == {"A": 1, "B": 3}

    # Restart on the same file: nothing left to do, nothing re-run.
    store2 = _make_store(db_path)
    worker2 = _make_worker(store2)
    _drive(store2, worker2, job_id, clock)

    states = _item_states(store2, job_id)
    assert states["A"]["state"] == COMPLETED
    assert states["A"]["output"] == "out-A"
    assert states["B"]["state"] == FAILED
    assert calls == calls_before, "no qwen calls after restart"
    # _rollup_job: a mixed job (1 completed + 1 failed == total) settles to
    # "completed" — the contract is "terminal, never wedged in processing".
    job = store2.get_job(job_id)
    assert job["state"] == JOB_COMPLETED
    assert job["completed"] == 1 and job["failed"] == 1
