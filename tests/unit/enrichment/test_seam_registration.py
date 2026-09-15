"""The seam registry must never be empty on a production call path.

Regression: adapter registration is an import side effect of ``adapters.py``.
The facets path called ``seam.build_adapter("service_backed")`` with nobody
having imported it — ``KeyError: unknown adapter 'service_backed'; known
adapters: []`` — and it surfaced only on the FIRST real enrichment run
(2026-09-14). ``build_adapter`` now imports ``adapters`` lazily on first use;
these tests pin that guarantee on the exact production import paths.
"""

from __future__ import annotations

import importlib


def test_facets_path_can_build_the_service_adapter() -> None:
    """Import facets_batch FRESH (as the CLI does) and build its adapter."""
    fb = importlib.import_module("datalake.defs.enrichment.facets_batch")
    adapter = fb._service_adapter("http://127.0.0.1:8462", "qwen/qwen3.7-flash")
    assert type(adapter).__name__ == "ServiceBackedAdapter"


def test_all_production_call_paths_resolve_an_adapter() -> None:
    """Every module that calls build_adapter at runtime must be able to."""
    for mod in ("submit", "harvest", "sensor", "facets_batch"):
        importlib.import_module(f"datalake.defs.enrichment.{mod}")
    from datalake.defs.enrichment.seam import ADAPTER_REGISTRY, build_adapter

    assert "service_backed" in ADAPTER_REGISTRY
    adapter = build_adapter("service_backed", base_url="http://x", model="m")
    assert type(adapter).__name__ == "ServiceBackedAdapter"


def test_unknown_adapter_still_fails_loudly() -> None:
    """The lazy registration must not mask a genuinely unknown name."""
    from datalake.defs.enrichment.seam import build_adapter

    try:
        build_adapter("not_a_provider")
    except KeyError as exc:
        assert "not_a_provider" in str(exc)
    else:
        raise AssertionError("unknown adapter name must raise KeyError")


def test_both_adapters_satisfy_the_provider_protocol() -> None:
    """Runtime Protocol conformance. Regression: the Protocol declared
    ``is_terminal`` and NEITHER adapter implemented it — AttributeError on the
    first real run, AFTER the submit was already billed. A runtime_checkable
    Protocol check catches a missing method at build time, not at poll time."""
    from datalake.defs.enrichment.seam import ProviderAdapter, build_adapter

    service = build_adapter("service_backed", base_url="http://x", model="m")
    direct = build_adapter("direct_batch", model="m")
    assert isinstance(service, ProviderAdapter)
    assert isinstance(direct, ProviderAdapter)
    for adapter in (service, direct):
        assert adapter.is_terminal("completed") is True
        assert adapter.is_terminal("failed") is True
        assert adapter.is_terminal("pending") is False
        assert adapter.is_terminal("processing") is False
