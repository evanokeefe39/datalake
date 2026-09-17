"""Unit tests for the interval harvest sensor (ADR-0012 decision 2).

Proves the interval SHAPE (every tick re-checks the full in-flight set), the
distinct skip messages (US-EENG-4 AC4), the loud health gate (US-EENG-2), and
that only terminal partitions request a harvest run. Runs against a REAL
ephemeral DagsterInstance — in-flight state is recorded with exactly the
mechanism the drain uses (``report_runless_asset_event``).

boundary-mock-ok: the seam adapter is registered under a test name in the
production registry and reached through the real build_adapter/detect_provider
path; the ephemeral DagsterInstance is real.
"""

from __future__ import annotations

import importlib
import json
import logging

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    RunRequest,
    build_sensor_context,
)

import orchestration.defs.engine.provider as seam
from orchestration.defs.engine.harvest import discover_handles
from orchestration.defs.engine.partitions import (
    SUBMITTED_ASSET_NAME,
    partition_key,
)
from orchestration.defs.engine.sensor import enrichment_harvest_sensor

WORKLOAD = "content_classification"


class EnrichmentInstance:
    """A REAL ephemeral DagsterInstance plus the submit-side helper: a
    submitted materialization carrying the provider handle, recorded with
    exactly the mechanism the drain uses."""

    def __init__(self) -> None:
        self.raw = DagsterInstance.ephemeral()

    def submit(self, key: str, handle: str | None = None) -> None:
        self.raw.report_runless_asset_event(
            AssetMaterialization(
                asset_key=AssetKey(SUBMITTED_ASSET_NAME),
                partition=key,
                metadata={"handle": handle} if handle else None,
            )
        )


class FakeAdapter:
    """Test adapter behind the seam registry — poll/health scripted per test."""

    name = "fake_sensor"

    def __init__(self, *, healthy: bool = True, states: dict[str, str] | None = None):
        self.healthy = healthy
        self.states = states or {}
        self.poll_calls: list[str] = []

    def health(self) -> bool:
        return self.healthy

    def poll(self, handle: str) -> str:
        self.poll_calls.append(handle)
        return self.states.get(handle, "processing")

    def normalize_state(self, state: str) -> str:
        return state

    def is_terminal(self, state: str) -> bool:
        return state in {"completed", "failed"}


@pytest.fixture
def fake_adapter(monkeypatch: pytest.MonkeyPatch):
    """Route build_adapter(detect_provider()) at a scripted FakeAdapter."""
    holder: dict[str, FakeAdapter] = {}

    def factory(**kwargs):
        return holder["adapter"]

    adapters_mod = importlib.import_module("orchestration.defs.engine.service_backed")
    seam.register_adapter("fake_sensor_probe", factory)
    monkeypatch.setattr(adapters_mod, "detect_provider", lambda: "fake_sensor_probe")

    def install(adapter: FakeAdapter) -> FakeAdapter:
        holder["adapter"] = adapter
        return adapter

    return install


def _tick(inst: EnrichmentInstance, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO, logger="enrichment.sensor"):
        result = enrichment_harvest_sensor.evaluate_tick(
            build_sensor_context(instance=inst.raw)
        )
    return result, [r.getMessage() for r in caplog.records]


def _key(pid: str) -> str:
    return partition_key(WORKLOAD, 0, [pid])


def _terminal_partitions(result) -> set[str]:
    return set(json.loads(result.run_requests[0].tags["enrichment/harvest_partitions"]))


# ─────────────────────────────────────────────── the interval shape (D2)


class TestIntervalShape:
    def test_two_ticks_recheck_the_same_nonterminal_inflight_job(
        self, fake_adapter, caplog
    ) -> None:
        """The core ADR-0012 D2 proof: an asset_sensor fires once on a NEW
        materialization event and never re-checks; THIS sensor re-derives and
        re-polls the same non-terminal job on every tick."""
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        adapter = fake_adapter(FakeAdapter(states={"job-1": "processing"}))

        result1, _ = _tick(inst, caplog)
        result2, _ = _tick(inst, caplog)

        assert len(adapter.poll_calls) == 2  # re-checked on the SECOND tick
        assert result1.run_requests == result2.run_requests == []

    def test_nothing_in_flight_requests_nothing(self, fake_adapter, caplog) -> None:
        inst = EnrichmentInstance()
        adapter = fake_adapter(FakeAdapter())

        result, messages = _tick(inst, caplog)

        assert result.run_requests == []
        assert adapter.poll_calls == []
        assert any("nothing in flight" in m for m in messages)


# ─────────────────────────────── distinct skip messages (US-EENG-4 AC4)


class TestSkipMessages:
    def test_inflight_none_terminal_names_the_count(self, fake_adapter, caplog) -> None:
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        inst.submit(_key("P2"), handle="job-1")
        fake_adapter(FakeAdapter(states={"job-1": "processing"}))

        _, messages = _tick(inst, caplog)

        assert any("2 in flight, none terminal yet" in m for m in messages)

    def test_skip_message_is_distinct_from_nothing_in_flight(
        self, fake_adapter, caplog
    ) -> None:
        empty_inst = EnrichmentInstance()
        busy_inst = EnrichmentInstance()
        busy_inst.submit(_key("P1"), handle="job-1")
        fake_adapter(FakeAdapter(states={"job-1": "processing"}))

        _, empty_messages = _tick(empty_inst, caplog)
        caplog.clear()
        _, busy_messages = _tick(busy_inst, caplog)

        assert any("nothing in flight" in m for m in empty_messages)
        assert any("in flight, none terminal yet" in m for m in busy_messages)
        assert not any("nothing in flight" in m for m in busy_messages)


# ───────────────────────────────────────── terminal → RunRequest


class TestTerminalTriggers:
    def test_terminal_partition_yields_a_harvest_run_request(
        self, fake_adapter, caplog
    ) -> None:
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        inst.submit(_key("P2"), handle="job-2")
        fake_adapter(
            FakeAdapter(states={"job-1": "completed", "job-2": "processing"})
        )

        result, _ = _tick(inst, caplog)

        assert len(result.run_requests) == 1
        run = result.run_requests[0]
        assert isinstance(run, RunRequest)
        # Only the TERMINAL partition is covered, not the non-terminal one.
        assert _key("P1") in _terminal_partitions(result)
        assert _key("P2") not in _terminal_partitions(result)

    def test_terminal_handle_covers_all_its_partitions(
        self, fake_adapter, caplog
    ) -> None:
        """All-or-nothing per provider job (ADR-0012): every in-flight
        partition a terminal handle covers flips together."""
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        inst.submit(_key("P2"), handle="job-1")
        fake_adapter(FakeAdapter(states={"job-1": "failed"}))

        result, _ = _tick(inst, caplog)

        assert len(result.run_requests) == 1
        assert _terminal_partitions(result) == {_key("P1"), _key("P2")}

    def test_run_key_dedupes_the_same_terminal_set(self, fake_adapter, caplog) -> None:
        """Consecutive ticks observing the same (not yet harvested) terminal
        set request nothing new."""
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        fake_adapter(FakeAdapter(states={"job-1": "completed"}))

        result1, _ = _tick(inst, caplog)
        result2, _ = _tick(inst, caplog)

        assert result1.run_requests[0].run_key == result2.run_requests[0].run_key


# ─────────────────────────────────────────── loud health gate (US-EENG-2)


class TestHealthGate:
    def test_down_service_with_work_in_flight_raises(self, fake_adapter) -> None:
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        fake_adapter(FakeAdapter(healthy=False))

        with pytest.raises(RuntimeError, match="readiness gate"):
            enrichment_harvest_sensor.evaluate_tick(
                build_sensor_context(instance=inst.raw)
            )

    def test_down_service_names_the_inflight_count(self, fake_adapter) -> None:
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        inst.submit(_key("P2"), handle="job-1")
        fake_adapter(FakeAdapter(healthy=False))

        with pytest.raises(RuntimeError, match="2 partition\\(s\\) in flight"):
            enrichment_harvest_sensor.evaluate_tick(
                build_sensor_context(instance=inst.raw)
            )


# ─────────────────────────────────────────── shared derivations


class TestSharedDerivation:
    def test_sensor_uses_the_same_in_flight_derivation_as_the_drain(self) -> None:
        """One definition of in-flight: the sensor reads
        partitions.in_flight_partitions, never a second copy."""
        import inspect

        import orchestration.defs.engine.sensor as sensor_mod

        src = inspect.getsource(sensor_mod.enrichment_harvest_sensor)
        assert "in_flight_partitions(instance)" in src

    def test_sensor_never_lands_or_mints(self) -> None:
        """ADR-0014 D2/D3: harvested reporting and retry minting are the
        harvest run's responsibilities — the sensor only triggers."""
        import inspect

        import orchestration.defs.engine.sensor as sensor_mod

        src = inspect.getsource(sensor_mod)
        for forbidden in (
            "land_result",
            "report_harvested",
            "mint_retries",
            ".retrieve(",
        ):
            assert forbidden not in src, f"sensor must not call {forbidden}"

    def test_sensor_reads_handles_as_the_harvest_run_does(
        self, fake_adapter, caplog
    ) -> None:
        inst = EnrichmentInstance()
        inst.submit(_key("P1"), handle="job-1")
        fake_adapter(FakeAdapter(states={"job-1": "completed"}))

        _tick(inst, caplog)

        assert discover_handles(inst.raw) == {_key("P1"): "job-1"}
