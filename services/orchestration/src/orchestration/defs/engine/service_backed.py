"""Service-backed enrichment adapter — the ONLY provider on the paid path.

The inference service owns an async job store; this adapter speaks its three
verbs over HTTP (`POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/results`) and
maps the service's job states onto the seam's canonical vocabulary. The
service is what makes a synchronous provider async, so this adapter is the
whole provider story: the Gemini batch path was retired (ADR-0009).

`PROVIDER_NAME` is the name callers pass to `build_adapter`. Registration is an
import side effect of this module, and `provider.build_adapter` imports it
lazily on first use so no call site can reach an empty registry.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

import httpx

from orchestration.defs.engine.provider import (
    _TERMINAL_STATUS,
    COMPLETED,
    DEFAULT_JOBSPEC,
    FAILED,
    PENDING,
    PROCESSING,
    RETRYABLE,
    TERMINAL,
    TERMINAL_STATES,
    UNKNOWN,
    Capabilities,
    Item,
    JobSpec,
    ProviderError,
    Result,
)
from orchestration.defs.integration.batch_client import DEFAULT_JOBS_SERVICE_URL

_DEFAULT_QWEN_MODEL = "qwen/qwen3.7-flash"

#: The name callers pass to `build_adapter`. It is the SEAM's name for this
#: adapter, not the provider's: the service behind it may change.
PROVIDER_NAME = "service_backed"
_DEFAULT_GEMINI_MODEL = "gemini-2.5-flash-lite"


# ─────────────────────────────────────────────────────── shared HTTP transport


class _HttpAdapter:
    """Shared transport + shared error-classification policy. Subclasses
    differ only where the providers genuinely differ."""

    name: str = "unnamed"
    model: str = ""
    capabilities: Capabilities

    def __init__(self, base_url: str, model: str, timeout_s: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.Client(timeout=timeout_s)

    # -- transport
    def _get(self, path: str) -> dict[str, Any]:
        try:
            resp = self._client.get(f"{self.base_url}{path}")
        except httpx.HTTPError as exc:
            raise ProviderError(f"transport error on GET {path}: {exc}") from exc
        if resp.status_code >= 400:
            raise ProviderError(
                f"GET {path} -> {resp.status_code}", status_code=resp.status_code
            )
        return resp.json()

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            resp = self._client.post(f"{self.base_url}{path}", json=body)
        except httpx.HTTPError as exc:
            raise ProviderError(f"transport error on POST {path}: {exc}") from exc
        if resp.status_code >= 400:
            raise ProviderError(
                f"POST {path} -> {resp.status_code}", status_code=resp.status_code
            )
        return resp.json()

    # -- shared policy
    def classify_error(self, exc: BaseException) -> str:
        """4xx-in-_TERMINAL_STATUS is the caller's fault; transient transport
        failures and other statuses are worth another attempt. Anything we
        cannot classify returns UNKNOWN — the caller must FAIL LOUDLY on
        UNKNOWN, never silently treat it as terminal (Gemini SDK errors
        must not masquerade as terminal just because they are foreign)."""
        if isinstance(exc, ProviderError):
            if exc.status_code is not None and exc.status_code in _TERMINAL_STATUS:
                return TERMINAL
            return RETRYABLE
        if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
            return RETRYABLE
        return UNKNOWN

    def is_terminal(self, state: str) -> bool:
        """Canonical-vocabulary predicate (Protocol seam.py:121). Terminal means
        COMPLETED or FAILED — the only two states a consumer may act on.
        Regression: the Protocol declared this method, neither adapter
        implemented it, and the facets path failed with AttributeError on the
        first real enrichment run (2026-09-14) AFTER the submit had already
        been billed."""
        return state in TERMINAL_STATES


def handle_codec(names_or_handle: str | Sequence[str]) -> str | list[str]:
    """THE handle encoding (batch contract): a JSON array of provider job
    names, produced AND parsed by this one function. A str input is a
    composite handle to parse (returns list[str]); a sequence input is a
    set of provider job names to encode (returns the JSON str). Single-
    chunk and multi-chunk handles share this exact representation — no
    ``,``/``|`` divergence."""
    if isinstance(names_or_handle, str):
        return json.loads(names_or_handle)
    return json.dumps(list(names_or_handle))


# ─────────────────────────────────────────────── adapter 1: service-backed
# Native vocabulary confirmed from the service's job store (the service's only
# job states) surfaced verbatim by GET /jobs/{id} → {"state": ...}.

_SERVICE_STATES = {
    "pending": PENDING,
    "processing": PROCESSING,
    "completed": COMPLETED,
    "failed": FAILED,
}


class ServiceBackedAdapter(_HttpAdapter):
    """An executor with NO job model of its own, wrapped in the inference
    service's async-job contract. The service owns state; we only poll it.
    Its transport is plain HTTP against the service routes — nothing from
    the direct-batch side is reachable from this adapter."""

    name = "service_backed"
    capabilities = Capabilities(
        supports_async_batch=True,
        requires_tier_gate=False,
        supports_chunking=False,
        media_resolution="low",
        native_state_vocabulary="pending|processing|completed|failed",
        notes="model executor behind the service's async-job contract",
    )

    def __init__(
        self,
        base_url: str | None = None,
        model: str = _DEFAULT_QWEN_MODEL,
        timeout_s: float = 60.0,
    ) -> None:
        super().__init__(
            base_url
            or os.environ.get("JOBS_SERVICE_URL", DEFAULT_JOBS_SERVICE_URL),
            model,
            timeout_s,
        )

    def build_request(self, item: Item) -> dict[str, Any]:
        return {
            "custom_key": item.custom_key,
            "prompt": item.prompt,
            "images": list(item.images),
        }

    def require_health(self) -> None:
        """US-EENG-2: the LOUD /health precondition, at the adapter boundary.
        A down service RAISES here — callers upstream can never mistake a
        dead executor for quiet 'nothing to do'."""
        try:
            ok = self._client.get(f"{self.base_url}/health").status_code == 200
        except httpx.HTTPError as exc:
            raise ProviderError(
                f"inference service {self.base_url} is DOWN (/health: {exc}); "
                "refusing to submit"
            ) from exc
        if not ok:
            raise ProviderError(
                f"inference service {self.base_url} health check FAILED; "
                "refusing to submit"
            )

    def submit(self, items: Sequence[Item], *, job_spec: JobSpec = DEFAULT_JOBSPEC) -> str:
        self.require_health()
        body: dict[str, Any] = {
            "items": [self.build_request(i) for i in items],
            "model": self.model,
        }
        if job_spec.max_tokens is not None:
            body["max_tokens"] = job_spec.max_tokens
        return str(self._post("/jobs", body)["job_id"])

    def poll(self, handle: str) -> dict[str, Any]:
        return self._get(f"/jobs/{handle}")

    def normalize_state(self, raw: Any) -> str:
        state = raw["state"]
        try:
            return _SERVICE_STATES[state]
        except KeyError:
            raise ProviderError(
                f"unrecognized jobs service job state: {state!r}"
            ) from None

    def retrieve(self, handle: str) -> list[Result]:
        raw = self._get(f"/jobs/{handle}/results")
        return [
            Result(
                custom_key=i["custom_key"],
                ok=bool(i["ok"]),
                response_text=i["output"],
                error=i["error"],
                model=self.model,
                provider=self.name,
            )
            for i in raw["items"]
        ]

    def health(self) -> bool:
        try:
            return self._client.get(f"{self.base_url}/health").status_code == 200
        except httpx.HTTPError:
            return False


# ─────────────────────────────────────────────────────────────── registration

from orchestration.defs.engine.provider import register_adapter  # noqa: E402

register_adapter(PROVIDER_NAME, ServiceBackedAdapter)
