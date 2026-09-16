"""Store projection + worker resilience tests (service review fixes).

Covers:
- list_jobs projects every job column (the worker reads max_tokens/max_attempts).
- create_job's max_tokens default matches DEFAULT_MAX_TOKENS (no drift).
- An unexpected exception in a worker tick resolves the item terminally
  instead of wedging it in `processing`.
- Lease reclaim: a stale `processing` claim past the lease window is retried
  while attempts remain, and failed once attempts are exhausted.
"""
from __future__ import annotations

import pytest
from jobs.store import (
    DEFAULT_MAX_TOKENS,
    FAILED,
    JOB_FAILED,
    Store,
)
from jobs.worker import Worker


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(tmp_path / "state.sqlite")


def _job(store: Store, *, max_tokens: int = 1234, max_attempts: int = 5) -> str:
    job_id, _ = store.create_job(
        [{"custom_key": "k1", "prompt": "p", "images": []}],
        model="test-model",
        max_tokens=max_tokens,
        max_attempts=max_attempts,
    )
    return job_id


def test_list_jobs_projects_max_tokens_and_attempts(store: Store):
    job_id = _job(store, max_tokens=4321, max_attempts=2)

    jobs = store.list_jobs()

    assert len(jobs) == 1
    assert jobs[0]["id"] == job_id
    assert jobs[0]["max_tokens"] == 4321
    assert jobs[0]["max_attempts"] == 2


def test_create_job_default_max_tokens_matches_constant(store: Store):
    job_id, _ = store.create_job(
        [{"custom_key": "k1", "prompt": "p"}], model="test-model"
    )

    assert store.get_job(job_id)["max_tokens"] == DEFAULT_MAX_TOKENS


def test_worker_tick_unexpected_exception_resolves_item_terminally(
    store: Store, monkeypatch: pytest.MonkeyPatch
):
    job_id = _job(store)
    w = Worker(store)

    def boom(self, job, item):  # noqa: ANN001 (test double)
        raise RuntimeError("simulated worker bug")

    monkeypatch.setattr(Worker, "_process", boom)
    assert w._tick() is True  # claimed and terminally failed in the same tick

    rows = store.job_items(job_id)
    assert rows[0]["state"] == FAILED
    assert "internal error" in rows[0]["error"]
    assert "simulated worker bug" in rows[0]["error"]
    # The job must not be left wedged in `processing`.
    job = store.get_job(job_id)
    assert job["failed"] == 1
    assert job["state"] == JOB_FAILED


def test_lease_reclaim_retries_stale_processing_item(store: Store):
    job_id = _job(store, max_attempts=3)
    item = store.claim_item(job_id, now=1000.0)
    assert item is not None

    # Before the lease expires the claim is held: nothing to claim.
    assert store.claim_item(job_id, now=1005.0) is None

    reclaimed = store.claim_item(job_id, now=1000.0 + 301.0)
    assert reclaimed is not None
    assert reclaimed.id == item.id
    assert reclaimed.attempts == 2  # reclaim counts as a fresh attempt


def test_lease_expiry_with_exhausted_attempts_fails_item(store: Store):
    job_id = _job(store, max_attempts=1)
    item = store.claim_item(job_id, now=1000.0)
    assert item is not None
    assert item.attempts == 1  # == max_attempts: worker dies before resolving

    # Past the lease there are no attempts left: the item must be failed so
    # the job can roll up instead of wedging in `processing` forever.
    assert store.claim_item(job_id, now=1000.0 + 301.0) is None

    rows = store.job_items(job_id)
    assert rows[0]["state"] == FAILED
    assert "lease" in rows[0]["error"]
    job = store.get_job(job_id)
    assert job["failed"] == 1
    assert job["state"] == JOB_FAILED
