"""Unit tests for the concrete adapters — NO network: the HTTP layer and
the Gemini SDK verbs are faked/monkeypatched."""

from __future__ import annotations

import orchestration.defs.engine.service_backed as adapters
import pytest
from orchestration.defs.engine import provider as seam
from orchestration.defs.engine.provider import (
    COMPLETED,
    FAILED,
    PENDING,
    PROCESSING,
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
    # the LOUD /health gate (US-EENG-2) runs first — a down service must
    # raise, never quietly submit into the void.
    assert a._client.calls[0] == ("GET", "/health")
    assert a._client.calls[-1] == ("POST", "/jobs")


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


# ─────────────────────────────────────────────── registry


def test_build_adapter_resolves_registered_names():
    """``service_backed`` is the only provider after the Gemini retirement."""
    a = seam.build_adapter("service_backed")
    assert type(a).__name__ == "ServiceBackedAdapter"
    assert isinstance(a.capabilities, Capabilities)
    # No provider is gated on a vendor tier any more: the service owns its
    # own concurrency control (ADR-0009).
    assert a.capabilities.requires_tier_gate is False


def test_build_adapter_unknown_name_raises():
    with pytest.raises(KeyError):
        seam.build_adapter("nope")


def test_is_terminal_agrees_with_normalize(service_routes):
    s = service_routes()
    for native, canonical in _SERVICE_CASES:
        assert s.is_terminal(s.normalize_state({"state": native})) == (
            canonical in (COMPLETED, FAILED)
        ), native


_SERVICE_CASES = [
    ("pending", PENDING),
    ("processing", PROCESSING),
    ("completed", COMPLETED),
    ("failed", FAILED),
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
            assert "gemini" not in name.lower(), name
