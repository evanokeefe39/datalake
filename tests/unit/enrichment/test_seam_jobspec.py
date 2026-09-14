"""Enrichment v3 seam-contract tests: JobSpec submit, ONE handle encoding,
ONE qwen transport through the adapter, UNKNOWN error classification.

NO network: the HTTP layer and the Gemini SDK verbs are faked/monkeypatched.
"""

from __future__ import annotations

import importlib
import json
import re
from pathlib import Path

import httpx
import pytest

from datalake.defs.enrichment import adapters, facets_batch, seam
from datalake.defs.enrichment.seam import (
    RETRYABLE,
    TERMINAL,
    UNKNOWN,
    Item,
    JobSpec,
    ProviderError,
)

SRC = Path(__file__).resolve().parents[3] / "src"

# boundary-mock-ok: the qwen-batch service is a separate localhost process
# (127.0.0.1:8462) and Gemini is a paid-tier cloud API — unit tests assert
# the SEAM CONTRACT (request shape, state mapping, handle encoding, error
# classification) against a transport fake that mirrors the real service
# routes in qwen_batch/app.py and the real SDK verbs in gemini_batch.py.
# End-to-end truth for the live path is the one observed cycle, not a unit
# test dialing a real LLM.

ITEMS = [
    Item(custom_key="p1", prompt="hi", post_id="p1", platform="instagram"),
    Item(custom_key="p2", prompt="yo", post_id="p2", platform="instagram"),
]


def gb():
    return importlib.import_module("datalake.defs.enrichment.gemini_batch")


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeClient:
    def __init__(self, routes: dict[tuple[str, str], FakeResponse]):
        self.routes = routes
        self.calls: list[tuple[str, str, dict | None]] = []

    def get(self, url: str, **kw) -> FakeResponse:
        path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
        self.calls.append(("GET", path, None))
        return self.routes.get(("GET", path), FakeResponse(404, {"detail": "nf"}))

    def post(self, url: str, json: dict | None = None, **kw) -> FakeResponse:
        path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
        self.calls.append(("POST", path, json))
        return self.routes.get(("POST", path), FakeResponse(404, {"detail": "nf"}))


def make_service_adapter(monkeypatch, health_status=200, health_exc=None):
    routes: dict[tuple[str, str], FakeResponse] = {
        ("GET", "/health"): FakeResponse(health_status, {}),
        ("POST", "/jobs"): FakeResponse(200, {"job_id": "job-1"}),
    }
    client = FakeClient(routes)
    if health_exc is not None:

        def boom(url, **kw):
            raise health_exc

        client.get = boom  # type: ignore[method-assign]
    monkeypatch.setattr(adapters.httpx, "Client", lambda **kw: client)
    return adapters.ServiceBackedAdapter(base_url="http://fake"), client


# ─────────────────────────────────────────────── handle encoding (ONE function)


def test_handle_codec_multi_chunk_round_trip():
    names = ["projects/x/locations/y/batches/jobs-aaa", "projects/x/batches/jobs-bbb"]
    handle = adapters.handle_codec(names)
    # JSON array — parses with plain json, no join-delimiter ambiguity
    assert json.loads(handle) == names
    assert adapters.handle_codec(handle) == names


def test_handle_codec_single_chunk_round_trip():
    names = ["projects/x/batches/jobs-only"]
    handle = adapters.handle_codec(names)
    assert json.loads(handle) == names
    assert adapters.handle_codec(handle) == names


def test_handle_codec_multi_chunk_beats_delimiter_encoding():
    # the old ',' join cannot distinguish one name containing a comma;
    # the JSON array encoding can.
    names = ["a,b", "c"]
    handle = adapters.handle_codec(names)
    assert adapters.handle_codec(handle) == ["a,b", "c"]


def test_direct_batch_round_trip_multi_chunk(monkeypatch):
    names = ["jobs/aaa", "jobs/bbb"]
    g = gb()
    monkeypatch.setattr(g, "submit", lambda *a, **k: names)
    monkeypatch.setattr(
        g,
        "poll",
        lambda gemini, name: {
            "state": "JOB_STATE_SUCCEEDED"
            if name == "jobs/aaa"
            else "JOB_STATE_RUNNING"
        },
    )
    monkeypatch.setattr(
        g, "job_state", lambda raw: raw["state"].removeprefix("JOB_STATE_")
    )

    a = adapters.DirectBatchAdapter()
    handle = a.submit(ITEMS, job_spec=JobSpec(max_tokens=512, mode="visual"))
    assert handle == json.dumps(names)  # JSON array, not 'jobs/aaa,jobs/bbb'
    raw = a.poll(handle)
    assert a.normalize_state(raw) == seam.PROCESSING


# ─────────────────────────────────────────────── JobSpec submit


def test_submit_expresses_max_tokens_without_ctor_kwargs(monkeypatch):
    a, client = make_service_adapter(monkeypatch)
    a.submit(ITEMS, job_spec=JobSpec(max_tokens=4096, mode="visual"))
    posts = [c for c in client.calls if c[0] == "POST"]
    assert len(posts) == 1
    body = posts[0][2]
    assert body["max_tokens"] == 4096
    assert body["model"] == a.model


def test_submit_default_jobspec_omits_max_tokens(monkeypatch):
    a, client = make_service_adapter(monkeypatch)
    a.submit(ITEMS)  # job_spec defaults — Protocol-compatible
    body = [c for c in client.calls if c[0] == "POST"][0][2]
    assert "max_tokens" not in body


def test_direct_batch_job_spec_overrides_ctor_default(monkeypatch):
    g = gb()
    captured = {}

    def fake_submit(gemini, model, requests, display_name, *, max_tokens=None):
        captured["max_tokens"] = max_tokens
        return ["jobs/x"]

    monkeypatch.setattr(g, "submit", fake_submit)
    a = adapters.DirectBatchAdapter(max_tokens=128)
    a.submit(ITEMS, job_spec=JobSpec(max_tokens=999))
    assert captured["max_tokens"] == 999


# ─────────────────────────────────────────────── LOUD health precondition


def test_submit_raises_loudly_when_service_down(monkeypatch):
    a, client = make_service_adapter(monkeypatch, health_status=503)
    with pytest.raises(ProviderError, match="health"):
        a.submit(ITEMS)
    assert not any(c[0] == "POST" for c in client.calls)  # never a quiet submit


def test_submit_raises_loudly_on_transport_error(monkeypatch):
    a, client = make_service_adapter(
        monkeypatch, health_exc=httpx.ConnectError("refused")
    )
    with pytest.raises(ProviderError, match="DOWN"):
        a.submit(ITEMS)
    assert not any(c[0] == "POST" for c in client.calls)


def test_facets_submit_health_gate_is_at_adapter(monkeypatch):
    """submit_facets_batch raises loudly when the service is down, via the
    adapter — and never touches qwen_client directly."""
    make_service_adapter(monkeypatch, health_status=500)
    with pytest.raises(ProviderError, match="health"):
        facets_batch.submit_facets_batch(
            [{"custom_key": "p1", "prompt": "hi", "images": []}],
            mode="text",
            base_url="http://fake",
        )


# ─────────────────────────────────────────────── error classification


def test_classify_error_unknown_exceptions_are_not_terminal():
    a = adapters.ServiceBackedAdapter(base_url="http://fake")
    assert a.classify_error(ValueError("foreign")) == UNKNOWN
    assert a.classify_error(RuntimeError("sdk exploded")) == UNKNOWN
    # known classes keep their policy
    assert a.classify_error(ProviderError("bad request", status_code=400)) == TERMINAL
    assert a.classify_error(ProviderError("server", status_code=500)) == RETRYABLE


# ─────────────────────────────────────────────── provider-name containment


def test_facets_batch_has_zero_direct_qwen_client_calls():
    text = (SRC / "datalake/defs/enrichment/facets_batch.py").read_text(
        encoding="utf-8"
    )
    assert "qwen_client." not in text
    assert "build_adapter" in text or "_service_adapter" in text


ADAPTER_LAYER = {
    "datalake/defs/enrichment/adapters.py",
    "datalake/defs/enrichment/qwen_client.py",
    "datalake/defs/enrichment/gemini_batch.py",
    "datalake/defs/enrichment/seam.py",
}


def test_provider_names_appear_only_inside_adapter_modules():
    """Grep-shaped: `gemini_batch.` / `qwen_client.` may appear ONLY inside
    the adapter layer across all of src/."""
    offenders = []
    for path in SRC.rglob("*.py"):
        rel = path.relative_to(SRC).as_posix()
        if rel in ADAPTER_LAYER:
            continue
        text = path.read_text(encoding="utf-8")
        code_lines = [
            ln
            for ln in text.splitlines()
            if not re.match(r'^\s*(["\'].*)$', ln)  # pure string-literal lines
        ]  # e.g. hermeticity marker tuples (_API_MARKERS) — data, not calls
        if re.search(r"\b(gemini_batch|qwen_client)\.", "\n".join(code_lines)):
            offenders.append(rel)
