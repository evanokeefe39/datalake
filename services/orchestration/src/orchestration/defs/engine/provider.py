"""The inference seam (ADR-0008/0009, ADR-0013): ONE Protocol, ONE lifecycle.

Ported verbatim in shape from the proven spike
(`~/repos/enrichment-spike/spike_defs/adapter.py`). This module is
provider-neutral BY CONSTRUCTION: it imports no provider SDK, no Dagster, and
no concrete adapter. Concrete adapters (`ServiceBackedAdapter` for the qwen
batch service, `DirectBatchAdapter` for Gemini) register themselves into
`ADAPTER_REGISTRY` — `build_adapter` is the only place a provider is named.

The seam keeps NO ledger (ADR-0013): the service owns its job store and the
caller polls. Failures surface as `landed(bronze) ∖ conformed(silver)`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

# --------------------------------------------------------- canonical vocabulary
# ONE vocabulary. Every adapter normalizes INTO this; nothing downstream ever
# sees a provider-native state string.

PENDING = "pending"
PROCESSING = "processing"
COMPLETED = "completed"
FAILED = "failed"

CANONICAL_STATES = frozenset({PENDING, PROCESSING, COMPLETED, FAILED})
TERMINAL_STATES = frozenset({COMPLETED, FAILED})

RETRYABLE = "retryable"
TERMINAL = "terminal"
# Unclassifiable error — consumers must FAIL LOUDLY, never treat as terminal.
UNKNOWN = "unknown"

# HTTP statuses that make a retry pointless — a terminal error class.
_TERMINAL_STATUS = frozenset({400, 401, 403, 404, 422})


class ProviderError(RuntimeError):
    """An adapter failure carrying enough detail to classify it."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class Item:
    """One unit of work, provider-neutral."""

    custom_key: str
    prompt: str
    images: tuple[str, ...] = ()
    post_id: str = ""
    platform: str = ""


@dataclass(frozen=True)
class Result:
    """One retrieved result, provider-neutral.

    `response_text` is the model output VERBATIM, whatever the provider calls
    it (`output` on one shape, `text` on the other both land here unchanged) —
    this is what the bronze landing stores, so it must never be lossy.
    Post identity and model/provider provenance travel beside it so bronze
    rows are self-describing.
    """

    custom_key: str
    ok: bool
    response_text: str | None
    error: str | None
    post_id: str = ""
    platform: str = ""
    model: str = ""
    provider: str = ""


@dataclass(frozen=True)
class Capabilities:
    """What an adapter can and cannot do. Consumers branch on THIS, not on name."""

    supports_async_batch: bool
    requires_tier_gate: bool
    supports_chunking: bool
    media_resolution: str
    native_state_vocabulary: str = ""
    notes: str = ""


@dataclass(frozen=True)
class JobSpec:
    """Per-CALL job-level options (batch contract). Carries what belongs to
    the whole provider job — not to any single item — so callers express it
    at submit time instead of smuggling it through constructor kwargs."""

    max_tokens: int | None = None
    mode: str | None = None


# The one shared empty JobSpec — frozen, so one instance serves every call.
DEFAULT_JOBSPEC = JobSpec()



@runtime_checkable
class ProviderAdapter(Protocol):
    """The seam. Implementations differ; consumers never do."""

    name: str
    model: str
    capabilities: Capabilities

    def build_request(self, item: Item) -> dict[str, Any]: ...
    def submit(self, items: Sequence[Item], *, job_spec: JobSpec = DEFAULT_JOBSPEC) -> str: ...
    def poll(self, handle: str) -> Any: ...
    def normalize_state(self, raw: Any) -> str: ...
    def is_terminal(self, state: str) -> bool: ...
    def retrieve(self, handle: str) -> list[Result]: ...
    def classify_error(self, exc: BaseException) -> str: ...


# ------------------------------------------------------------------- registry
# Concrete adapters attach here WITHOUT editing build_adapter's body:
#     from orchestration.defs.engine import provider
#     seam.register_adapter("service_backed", ServiceBackedAdapter)
# `build_adapter` remains the ONLY place a provider name is looked up.

ADAPTER_REGISTRY: dict[str, Callable[..., ProviderAdapter]] = {}


def register_adapter(name: str, factory: Callable[..., ProviderAdapter]) -> None:
    """Attach a concrete adapter factory (or class) under a config name.

    Preconditions: `name` non-empty; `factory` builds a `ProviderAdapter`.
    Re-registering the same name replaces the previous factory (last wins).
    """
    if not name:
        raise ValueError("adapter name must be non-empty")
    ADAPTER_REGISTRY[name] = factory


def build_adapter(name: str, **kwargs: Any) -> ProviderAdapter:
    """The ONLY place a provider is named. Swapping providers is this string.

    Raises KeyError naming every known adapter when `name` is unknown.
    """
    if name not in ADAPTER_REGISTRY:
        # Registration is an import side effect of adapters.py. Import it lazily
        # on FIRST USE so no production call site can reach an empty registry —
        # the facets path shipped exactly that bug (KeyError: known adapters:
        # []), caught by the first real enrichment run on 2026-09-14. Safe at
        # call time: adapters imports this module, which is fully initialized
        # by then. The import is WRAPPED so a failure mid-package-init falls
        # through to the KeyError below — the failure mode stays "loud KeyError
        # naming the empty registry", never an ImportError that masks it.
        try:
            from orchestration.defs.engine import service_backed  # noqa: F401, PLC0415
        except ImportError:  # pragma: no cover — only mid-package-init
            pass
    if name not in ADAPTER_REGISTRY:
        raise KeyError(
            f"unknown adapter {name!r}; known adapters: {sorted(ADAPTER_REGISTRY)}"
        )
    return ADAPTER_REGISTRY[name](**kwargs)


# ------------------------------------------------- the ONE shared lifecycle
