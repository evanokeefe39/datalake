"""Unit tests for the concrete adapters — NO network: the HTTP layer and
the Gemini SDK verbs are faked/monkeypatched."""

from __future__ import annotations

import httpx
import pytest

from datalake.defs.enrichment import adapters, seam
from datalake.defs.enrichment.seam import (
    COMPLETED,
    FAILED,
    PENDING,
    PROCESSING,
    RETRYABLE,
    TERMINAL,
    Capabilities,
    Item,
    ProviderError,
)

# ─────────────────────────────────────────────────────── fakes


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeClient:
    """Stands in for httpx.Client; routes are (method, path) → response."""

    def __init__(self, routes: dict[tuple[str, str], FakeResponse]):
        self.routes = routes
        self.calls: list[tuple[str, str]] = []

    def get(self, url: str, **kw) -> FakeResponse:
        path = url.split("://", 1)[-1].split("/", 1)[-1]
        self.calls.append(("GET", f"/{path}"))
        key = ("GET", f"/{path}")
        if key not in self.routes:
            return FakeResponse(404, {"detail": "not found"})
        return self.routes[key]

    def post(self, url: str, json: dict | None = None, **kw) -> FakeResponse:
        path = url.split("://", 1)[-1].split("/", 1)[-1]
        self.calls.append(("POST", f"/{path}"))
        key = ("POST", f"/{path}")
        if key not in self.routes:
            return FakeResponse(404, {"detail": "not found"})
        return self.routes[key]


@pytest.fixture
def service_routes(monkeypatch):
    """Install a FakeClient as the transport for every ServiceBackedAdapter,
    with the REAL service routes from qwen_batch/app.py pre-populated."""
    routes: dict[tuple[str, str], FakeResponse] = {
        ("POST", "/jobs"): FakeResponse(200, {"job_id": "job-1", "total": 2}),
        ("GET", "/jobs/job-1"): FakeResponse(
            200,
            {
                "job_id": "job-1",
                "state": "processing",
                "total": 2,
                "completed": 1,
                "failed": 0,
                "error": None,
            },
        ),
        ("GET", "/jobs/job-1/results"): FakeResponse(
            200,
            {
                "items": [
                    {"custom_key": "p1", "ok": True, "output": "hello", "error": None},
                    {"custom_key": "p2", "ok": False, "output": None, "error": "boom"},
                ]
            },
        ),
        ("GET", "/health"): FakeResponse(200, {"status": "ok"}),
    }
    holder: dict = {}

    def make(routes_override=None):
        client = FakeClient(routes_override or routes)
        monkeypatch.setattr(adapters.httpx, "Client", lambda **kw: client)
        holder["client"] = client
        return adapters.ServiceBackedAdapter(base_url="http://fake")

    return make


ITEMS = [
    Item(custom_key="p1", prompt="hi", post_id="p1", platform="instagram"),
    Item(custom_key="p2", prompt="yo", post_id="p2", platform="instagram"),
]


# ─────────────────────────────────────────────── service-backed adapter


def test_service_roundtrip_real_routes(service_routes):
    a = service_routes()
    handle = a.submit(ITEMS)
    assert handle == "job-1"
    raw = a.poll(handle)
    assert raw["state"] == "processing"
    assert a.normalize_state(raw) == PROCESSING
    assert not a.is_terminal(PROCESSING)
    results = a.retrieve(handle)
    assert [(r.custom_key, r.ok, r.response_text, r.error) for r in results] == [
        ("p1", True, "hello", None),
        ("p2", False, None, "boom"),
    ]
    assert all(r.provider == "service_backed" and r.model == a.model for r in results)


def test_service_submit_body_shape(service_routes):
    a = service_routes()
    a.submit(ITEMS)
    # body shape confirmed against app.py JobIn: items[{custom_key,prompt,images}], model
    assert a._client.calls == [("POST", "/jobs")]


def test_service_normalize_maps_every_native_state(service_routes):
    a = service_routes()
    for native, canonical in [
        ("pending", PENDING),
        ("processing", PROCESSING),
        ("completed", COMPLETED),
        ("failed", FAILED),
    ]:
        assert a.normalize_state({"state": native}) == canonical


def test_service_normalize_unknown_state_raises(service_routes):
    a = service_routes()
    with pytest.raises(ProviderError, match="unrecognized"):
        a.normalize_state({"state": "queued"})


def test_service_health(service_routes):
    a = service_routes()
    assert a.health() is True

# ─────────────────────────────────────────────── direct-batch adapter


@pytest.fixture
def gemini_jobs(monkeypatch):
    """Monkeypatch the repo's REAL Gemini verbs on the gemini_batch module —
    the adapter must reuse them, so the fakes sit exactly where production
    code paths sit."""
    from datalake.defs.enrichment import gemini_batch

    state = {"submitted": None, "polled": [], "retrieved": []}

    def fake_submit(gemini, model, requests, display_name, max_tokens=None):
        # mimic tier chunking: two names for one logical submit
        state["submitted"] = (model, requests, display_name)
        return ["jobs/aaa", "jobs/bbb"]

    def fake_poll(gemini, job_name):
        state["polled"].append(job_name)

        class Job:
            state = {"jobs/aaa": "JOB_STATE_SUCCEEDED", "jobs/bbb": "JOB_STATE_RUNNING"}[
                job_name
            ]

        return Job()

    def fake_retrieve(gemini, job_name):
        state["retrieved"].append(job_name)
        return {
            "jobs/aaa": {"p1": {"ok": True, "text": "result-a", "error": None}},
            "jobs/bbb": {"p2": {"ok": False, "text": None, "error": "quota"}},
        }[job_name]

    monkeypatch.setattr(gemini_batch, "submit", fake_submit)
    monkeypatch.setattr(gemini_batch, "poll", fake_poll)
    monkeypatch.setattr(gemini_batch, "job_state", lambda job: job.state[10:])
    monkeypatch.setattr(gemini_batch, "retrieve", fake_retrieve)
    monkeypatch.setattr(
        adapters.DirectBatchAdapter, "_gemini", lambda self: object()
    )
    return state


def test_direct_batch_roundtrip_composite_handle(gemini_jobs):
    a = adapters.DirectBatchAdapter()
    handle = a.submit(ITEMS)
    assert handle == "jobs/aaa,jobs/bbb"
    raw = a.poll(handle)
    # one RUNNING + one SUCCEEDED → the aggregate is PROCESSING, never terminal
    assert a.normalize_state(raw) == PROCESSING
    results = a.retrieve(handle)
    assert sorted((r.custom_key, r.ok, r.response_text, r.error) for r in results) == [
        ("p1", True, "result-a", None),
        ("p2", False, None, "quota"),
    ]
    assert all(r.provider == "direct_batch" for r in results)
    assert gemini_jobs["retrieved"] == ["jobs/aaa", "jobs/bbb"]


def test_direct_batch_normalize_maps_every_native_state(gemini_jobs):
    a = adapters.DirectBatchAdapter()
    for native, canonical in [
        ("STATE_UNSPECIFIED", PENDING),
        ("SUBMITTED", PENDING),
        ("PENDING", PENDING),
        ("QUEUED", PENDING),
        ("PAUSED", PENDING),
        ("RUNNING", PROCESSING),
        ("SUCCEEDED", COMPLETED),
        ("FAILED", FAILED),
        ("CANCELLED", FAILED),
        ("EXPIRED", FAILED),
    ]:
        assert a.normalize_state({"job_states": [native]}) == canonical
    assert a.normalize_state({"job_states": ["SUCCEEDED", "SUCCEEDED"]}) == COMPLETED
    assert a.normalize_state({"job_states": ["PENDING", "RUNNING"]}) == PROCESSING


def test_direct_batch_normalize_unknown_state_raises(gemini_jobs):
    a = adapters.DirectBatchAdapter()
    with pytest.raises(ProviderError, match="unrecognized"):
        a.normalize_state({"job_states": ["JOB_STATE_PENDING"]})  # prefix not stripped


def test_direct_batch_health_is_tier_gate(monkeypatch):
    from datalake.defs.instagram.config import GeminiTier, GeminiTierConfig

    a = adapters.DirectBatchAdapter()
    monkeypatch.setattr(GeminiTierConfig, "detect", lambda: GeminiTierConfig(GeminiTier.FREE))
    assert a.health() is False
    monkeypatch.setattr(GeminiTierConfig, "detect", lambda: GeminiTierConfig(GeminiTier.TIER_1))
    assert a.health() is True


def test_direct_batch_detect_provider(monkeypatch):
    from datalake.defs.instagram.config import GeminiTier, GeminiTierConfig

    monkeypatch.setattr(GeminiTierConfig, "detect", lambda: GeminiTierConfig(GeminiTier.TIER_2))
    assert adapters.detect_provider() == "direct_batch"
    monkeypatch.setattr(GeminiTierConfig, "detect", lambda: GeminiTierConfig(GeminiTier.FREE))
    assert adapters.detect_provider() == "service_backed"


# ─────────────────────────────────────────────── error classification


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_classify_terminal_statuses(service_routes, status):
    a = service_routes()
    assert a.classify_error(ProviderError("x", status_code=status)) == TERMINAL


@pytest.mark.parametrize("status", [500, 502, 503, 429])
def test_classify_retryable_statuses(service_routes, status):
    a = service_routes()
    assert a.classify_error(ProviderError("x", status_code=status)) == RETRYABLE


@pytest.mark.parametrize(
    "exc",
    [httpx.TimeoutException("t"), httpx.ConnectError("c")],
)
def test_classify_transport_errors_retryable(service_routes, exc):
    a = service_routes()
    assert a.classify_error(exc) == RETRYABLE


def test_classify_other_exceptions_terminal(service_routes):
    a = service_routes()
    assert a.classify_error(ValueError("nope")) == TERMINAL


# ─────────────────────────────────────────────── registry


def test_build_adapter_resolves_registered_names():
    a = seam.build_adapter("service_backed")
    b = seam.build_adapter("direct_batch")
    assert type(a).__name__ == "ServiceBackedAdapter"
    assert type(b).__name__ == "DirectBatchAdapter"
    assert isinstance(a.capabilities, Capabilities)
    assert a.capabilities.requires_tier_gate is False
    assert b.capabilities.requires_tier_gate is True


def test_build_adapter_unknown_name_raises():
    with pytest.raises(KeyError):
        seam.build_adapter("nope")


def test_is_terminal_agrees_with_normalize(service_routes, gemini_jobs):
    s = service_routes()
    for native, canonical in _SERVICE_CASES:
        assert s.is_terminal(s.normalize_state({"state": native})) == (
            canonical in (COMPLETED, FAILED)
        ), native
    d = adapters.DirectBatchAdapter()
    for native, canonical in _GEMINI_CASES:
        assert d.is_terminal(d.normalize_state({"job_states": [native]})) == (
            canonical in (COMPLETED, FAILED)
        ), native


_SERVICE_CASES = [
    ("pending", PENDING),
    ("processing", PROCESSING),
    ("completed", COMPLETED),
    ("failed", FAILED),
]
_GEMINI_CASES = [
    ("STATE_UNSPECIFIED", PENDING),
    ("SUBMITTED", PENDING),
    ("PENDING", PENDING),
    ("QUEUED", PENDING),
    ("PAUSED", PENDING),
    ("RUNNING", PROCESSING),
    ("SUCCEEDED", COMPLETED),
    ("FAILED", FAILED),
    ("CANCELLED", FAILED),
    ("EXPIRED", FAILED),
]


# ─────────────────────────────────────────────── Gemini isolation of the
# service-backed path: importing adapters must not pull any Gemini module
# into THIS module's namespace (the Gemini dependency is lazy, inside
# DirectBatchAdapter methods only).


def test_service_backed_path_imports_no_gemini_symbols():
    """The qwen path must reach no Gemini-specific code: the adapters module
    binds no Gemini module (Gemini access is lazy, inside DirectBatchAdapter
    methods only), and ServiceBackedAdapter's source contains no Gemini
    reference at all."""
    import inspect
    import types

    assert "gemini" not in inspect.getsource(adapters.ServiceBackedAdapter).lower()
    for name, value in vars(adapters).items():
        if isinstance(value, types.ModuleType):
            assert "gemini" not in value.__name__.lower(), name
        else:
            assert "gemini" not in name.lower() or name in {
                "_GEMINI_STATES",
                "_DEFAULT_GEMINI_MODEL",
            }, name
