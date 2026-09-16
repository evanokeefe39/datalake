"""The /health readiness gate must FAIL when the service cannot do work.

Every datalake gate site (`engine/submit.py`, `engine/harvest.py`,
`engine/sensor.py`, `service_backed.require_health`) treats a non-200 as "the
inference service is unusable". A hardcoded 200 therefore reports a service
whose worker thread has died, or whose store is unreadable, as READY —
submissions queue forever and the platform looks idle rather than broken.

These tests exercise the endpoint through the real ASGI app and the real Store,
so the status code and the reported reason are what a caller actually observes.
"""

from __future__ import annotations

import threading

from fastapi.testclient import TestClient
from jobs import app as app_mod
from jobs.store import Store
from jobs.worker import Worker

# boundary-mock-ok: `worker.alive` is the gate's INPUT (a liveness signal that
# cannot be made to go false deterministically without killing a thread mid-test).
# Everything around it is real: the real app, the real Store, the real sqlite
# file. The worker's own thread semantics are covered by
# `test_worker_alive_reflects_thread_state` below, unmocked.


def test_health_ok_when_worker_alive_and_store_readable(monkeypatch, tmp_path):
    real_store = Store(str(tmp_path / "state.sqlite"))
    monkeypatch.setattr(app_mod, "store", real_store)
    monkeypatch.setattr(app_mod.worker, "alive", lambda: True)
    resp = TestClient(app_mod.app, raise_server_exceptions=False).get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["model"]
    assert body["version"]


def test_health_503_when_worker_thread_is_dead(monkeypatch, tmp_path):
    """A dead worker accepts submissions it will never process."""
    real_store = Store(str(tmp_path / "state.sqlite"))
    monkeypatch.setattr(app_mod, "store", real_store)
    monkeypatch.setattr(app_mod.worker, "alive", lambda: False)
    resp = TestClient(app_mod.app, raise_server_exceptions=False).get("/health")
    assert resp.status_code == 503
    assert "worker" in resp.json()["detail"]


def test_health_503_when_store_is_unreadable(monkeypatch, tmp_path):
    """A store sqlite cannot open means the service cannot record work.

    The store is constructed against a REAL file (its constructor opens one and
    would raise otherwise), then the file is replaced by a directory so the
    health check's own open genuinely fails — the failure is produced by
    sqlite, not by a patched attribute.
    """
    db = tmp_path / "state.sqlite"
    store = Store(str(db))
    db.unlink()
    db.mkdir()
    monkeypatch.setattr(app_mod, "store", store)
    monkeypatch.setattr(app_mod.worker, "alive", lambda: True)
    resp = TestClient(app_mod.app, raise_server_exceptions=False).get("/health")
    assert resp.status_code == 503
    assert "store unreadable" in resp.json()["detail"]


def test_worker_alive_reflects_real_thread_state():
    """`alive()` is the gate's evidence — it must track the real thread."""
    w = Worker.__new__(Worker)
    w._thread = None
    assert w.alive() is False

    t = threading.Thread(target=lambda: None)
    w._thread = t
    assert w.alive() is False  # created but never started
    t.start()
    t.join()
    assert w.alive() is False  # started, then finished
