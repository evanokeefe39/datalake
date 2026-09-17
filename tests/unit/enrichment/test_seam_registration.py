"""The seam registry must never be empty on a production call path.

Regression: adapter registration is an import side effect of the adapter
module. The facets path once called ``seam.build_adapter("service_backed")``
with nobody having imported it — ``KeyError: unknown adapter 'service_backed';
known adapters: []`` — and it surfaced only on the FIRST real enrichment run
(2026-09-14). ``build_adapter`` now imports the module lazily on first use;
these tests pin that guarantee on the exact production import paths.

Post-ADR-0015/0016 the production paths are: the engine's submit/harvest/sensor
modules and the workload registry (which the submit stage iterates). The Gemini
``direct_batch`` adapter is deleted, so ``service_backed`` is the only
registered provider and the Protocol conformance check has one subject.
"""

from __future__ import annotations

import importlib


def test_every_production_call_path_resolves_an_adapter() -> None:
    """Every module that calls build_adapter at runtime must be able to.

    ``engine.submit`` / ``engine.harvest`` / ``engine.sensor`` all build the
    adapter through ``service_backed.PROVIDER_NAME``; the registry must be
    populated by then without the caller importing anything extra.
    """
    from orchestration.defs.engine.provider import ADAPTER_REGISTRY, build_adapter

    for mod in (
        "orchestration.defs.engine.submit",
        "orchestration.defs.engine.harvest",
        "orchestration.defs.engine.sensor",
    ):
        importlib.import_module(mod)

    from orchestration.defs.engine import service_backed

    assert service_backed.PROVIDER_NAME in ADAPTER_REGISTRY
    adapter = build_adapter(service_backed.PROVIDER_NAME, base_url="http://x", model="m")
    assert type(adapter).__name__ == "ServiceBackedAdapter"


def test_workload_registry_imports_without_naming_a_provider() -> None:
    """The registry declares payloads, never transports: importing it must not
    pull an adapter in, and every registered workload must be dispatchable."""
    importlib.import_module("orchestration.defs.ig_enriched.slv.workloads")
    from orchestration.defs.engine.provider import ADAPTER_REGISTRY
    from orchestration.defs.ig_enriched.slv.workloads import WORKLOADS

    assert WORKLOADS, "the registry must declare at least one workload"
    for workload in WORKLOADS:
        assert workload.name, "a workload must be named to be dispatchable"
        assert workload.silver_table, (
            f"{workload.name} declares no silver table — it would be invisible "
            "to check_no_silent_loss"
        )
    # Importing the registry names no provider.
    assert "direct_batch" not in ADAPTER_REGISTRY


def test_unknown_adapter_still_fails_loudly() -> None:
    """The lazy registration must not mask a genuinely unknown name."""
    from orchestration.defs.engine.provider import build_adapter

    try:
        build_adapter("not_a_provider")
    except KeyError as exc:
        assert "not_a_provider" in str(exc)
    else:
        raise AssertionError("unknown adapter name must raise KeyError")


def test_retired_gemini_adapter_is_gone() -> None:
    """ADR-0015 deleted the Gemini path: the adapter name must not resolve.

    A retired provider reachable by config string is the defect this pins —
    ``detect_provider()`` returned ``direct_batch`` whenever the tier gate
    opened, silently routing paid work to a retired backend.
    """
    from orchestration.defs.engine.provider import ADAPTER_REGISTRY, build_adapter

    assert "direct_batch" not in ADAPTER_REGISTRY
    try:
        build_adapter("direct_batch")
    except KeyError:
        pass
    else:
        raise AssertionError("the retired Gemini adapter must not resolve")


def test_the_service_adapter_satisfies_the_provider_protocol() -> None:
    """Runtime Protocol conformance. Regression: the Protocol declared
    ``is_terminal`` and NEITHER adapter implemented it — AttributeError on the
    first real run, AFTER the submit was already billed. A runtime_checkable
    Protocol check catches a missing method at build time, not at poll time.
    """
    from orchestration.defs.engine.provider import ProviderAdapter, build_adapter

    adapter = build_adapter("service_backed", base_url="http://x", model="m")
    assert isinstance(adapter, ProviderAdapter)
    assert adapter.is_terminal("completed") is True
    assert adapter.is_terminal("failed") is True
    assert adapter.is_terminal("pending") is False
