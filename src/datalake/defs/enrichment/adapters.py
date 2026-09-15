"""Concrete provider adapters behind the seam (wave 2 of Enrichment v3).

Two adapters implement :class:`datalake.defs.enrichment.seam.ProviderAdapter`
against the REAL providers:

* :class:`ServiceBackedAdapter` — the qwen-batch-service REST contract
  (``POST /jobs``, ``GET /jobs/{id}``, ``GET /jobs/{id}/results``,
  ``GET /health`` — confirmed against ``qwen_batch/app.py``). No Gemini code
  is reachable from this adapter: its Gemini dependency is imported lazily
  inside :class:`DirectBatchAdapter` methods only.
* :class:`DirectBatchAdapter` — the real Gemini Batch API via the existing
  repo verbs in :mod:`datalake.defs.enrichment.gemini_batch` (create batch,
  poll job state, retrieve inlined/JSONL results). ``gemini_batch.submit``
  already chunks under the tier's in-flight token cap
  (``GeminiTierConfig.max_batch_tokens``), so one logical submit may map to
  SEVERAL provider jobs; the adapter encodes them into ONE opaque comma-
  joined handle — orchestration never learns it is composite (no separate
  ChunkedDirectBatchAdapter is needed; the chunking lives in the reused
  repo code path).

Registration happens on import of this module; ``build_adapter(name)`` is
the only name lookup. ADR-0013: the seam keeps NO ledger — neither adapter
records jobs anywhere.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

import httpx

from datalake.defs.enrichment.qwen_client import DEFAULT_QWEN_SERVICE_URL
from datalake.defs.enrichment.seam import (
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

_DEFAULT_QWEN_MODEL = "qwen/qwen3.7-flash"
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




def _media_files(item: Item) -> list[dict[str, Any]] | None:
    """Map seam ``Item.images`` onto gemini_batch ``media_files`` dicts.

    A bare string is a File API/media URI (scrape-time cached local path or
    File API uri); a dict passes through untouched (``{"uri", ...}`` or
    ``{"inline_data", "mime_type"}``).
    """
    if not item.images:
        return None
    return [m if isinstance(m, dict) else {"uri": m} for m in item.images]


# ─────────────────────────────────────────────── adapter 1: service-backed
# Native vocabulary confirmed from qwen_batch/store.py (the service's only
# job states) surfaced verbatim by GET /jobs/{id} → {"state": ...}.

_SERVICE_STATES = {
    "pending": PENDING,
    "processing": PROCESSING,
    "completed": COMPLETED,
    "failed": FAILED,
}


class ServiceBackedAdapter(_HttpAdapter):
    """An executor with NO job model of its own, wrapped in the qwen-batch
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
        notes="qwen executor behind the service's async-job contract",
    )

    def __init__(
        self,
        base_url: str | None = None,
        model: str = _DEFAULT_QWEN_MODEL,
        timeout_s: float = 60.0,
    ) -> None:
        super().__init__(
            base_url
            or os.environ.get("QWEN_SERVICE_URL", DEFAULT_QWEN_SERVICE_URL),
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
                f"qwen-batch service {self.base_url} is DOWN (/health: {exc}); "
                "refusing to submit"
            ) from exc
        if not ok:
            raise ProviderError(
                f"qwen-batch service {self.base_url} health check FAILED; "
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
                f"unrecognized qwen-batch-service job state: {state!r}"
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


# ─────────────────────────────────────────────── adapter 2: direct batch
# Native vocabulary = the short form ``gemini_batch.job_state`` strips to
# (it drops the ``JOB_STATE_`` prefix): the terminal + active sets in
# gemini_batch.py, confirmed against the repo's real Gemini Batch verbs.

_GEMINI_STATES = {
    "STATE_UNSPECIFIED": PENDING,
    "SUBMITTED": PENDING,
    "PENDING": PENDING,
    "QUEUED": PENDING,
    "PAUSED": PENDING,
    "RUNNING": PROCESSING,
    "SUCCEEDED": COMPLETED,
    "FAILED": FAILED,
    "CANCELLED": FAILED,
    "EXPIRED": FAILED,
}


class DirectBatchAdapter(_HttpAdapter):
    """A provider that ALREADY tracks jobs (Gemini Batch API). Never proxied
    through the service — that would duplicate job tracking.

    ``gemini_batch.submit`` chunks requests under the tier's in-flight token
    cap, so ``submit`` may create SEVERAL provider jobs; the handle is the
    shared JSON-array ``handle_codec`` encoding of those job names, opaque
    to callers. Gemini access is imported lazily so importing this module
    never pulls google-genai.
    """

    name = "direct_batch"
    capabilities = Capabilities(
        supports_async_batch=True,
        requires_tier_gate=True,
        supports_chunking=True,
        media_resolution="low",
        native_state_vocabulary=(
            "STATE_UNSPECIFIED|SUBMITTED|PENDING|QUEUED|PAUSED|RUNNING"
            "|SUCCEEDED|FAILED|CANCELLED|EXPIRED"
        ),
        notes="provider-owned job tracking; batch is paid-tier only",
    )

    def __init__(
        self,
        model: str = _DEFAULT_GEMINI_MODEL,
        max_tokens: int | None = None,
        display_name: str = "enrichment-batch",
    ) -> None:
        self.max_tokens = max_tokens
        self.display_name = display_name
        # No HTTP transport is needed — Gemini goes through the SDK verbs in
        # gemini_batch. There is deliberately NO silent base_url fallback here:
        # this adapter has no HTTP endpoint, and SDK errors classify as
        # UNKNOWN (fail loudly), never as terminal-by-default.
        self.model = model

    def _gemini(self):
        from datalake.defs.common.resources import GeminiResource

        return GeminiResource()

    def build_request(self, item: Item) -> dict[str, Any]:
        return {
            "custom_key": item.custom_key,
            "prompt": item.prompt,
            "media_files": _media_files(item),
        }

    def submit(self, items: Sequence[Item], *, job_spec: JobSpec = DEFAULT_JOBSPEC) -> str:
        from datalake.defs.enrichment import gemini_batch

        requests = [self.build_request(i) for i in items]
        names = gemini_batch.submit(
            self._gemini(),
            self.model,
            requests,
            self.display_name,
            max_tokens=(
                job_spec.max_tokens
                if job_spec.max_tokens is not None
                else self.max_tokens
            ),
        )
        return handle_codec(names)

    def poll(self, handle: str) -> dict[str, Any]:
        from datalake.defs.enrichment import gemini_batch

        gemini = self._gemini()
        job_states = [
            gemini_batch.job_state(gemini_batch.poll(gemini, name))
            for name in handle_codec(handle)
        ]
        return {"job_states": job_states}

    def normalize_state(self, raw: Any) -> str:
        """Aggregate a composite handle's native states into ONE canonical
        state. Every native state must be recognized — unknown raises."""
        mapped = []
        for state in raw["job_states"]:
            try:
                mapped.append(_GEMINI_STATES[state])
            except KeyError:
                raise ProviderError(
                    f"unrecognized Gemini batch job state: {state!r}"
                ) from None
        if FAILED in mapped:
            return FAILED
        if all(s == COMPLETED for s in mapped):
            return COMPLETED
        if PENDING in mapped and PROCESSING not in mapped and COMPLETED not in mapped:
            return PENDING
        return PROCESSING

    def retrieve(self, handle: str) -> list[Result]:
        from datalake.defs.enrichment import gemini_batch

        gemini = self._gemini()
        merged: dict[str, dict] = {}
        for name in handle_codec(handle):
            merged.update(gemini_batch.retrieve(gemini, name))
        return [
            Result(
                custom_key=key,
                ok=bool(v["ok"]),
                response_text=v["text"],
                error=v["error"],
                model=self.model,
                provider=self.name,
            )
            for key, v in merged.items()
        ]

    def health(self) -> bool:
        """The direct path's readiness gate is the tier gate, not a ping."""
        from datalake.defs.instagram.config import GeminiTierConfig

        return GeminiTierConfig.detect().supports_batch


# ───────────────────────────────────────────────── provider detection helper


def detect_provider() -> str:
    """Pick the config name for the active environment: direct Gemini Batch
    when the tier gate opens it, the service-backed path otherwise."""
    if DirectBatchAdapter().health():
        return "direct_batch"
    return "service_backed"


# ─────────────────────────────────────────────────────────────── registration

from datalake.defs.enrichment.seam import register_adapter  # noqa: E402

register_adapter("service_backed", ServiceBackedAdapter)
register_adapter("direct_batch", DirectBatchAdapter)
