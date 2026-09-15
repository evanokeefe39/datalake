"""Unit tests for the provider-neutral inference seam (no network, no service)."""

import pytest

from orchestration.defs.engine.provider import (
    CANONICAL_STATES,
    COMPLETED,
    DEFAULT_JOBSPEC,
    FAILED,
    PENDING,
    PROCESSING,
    TERMINAL_STATES,
    Capabilities,
    Item,
    ProviderAdapter,
    ProviderError,
    Result,
    build_adapter,
    register_adapter,
)
from orchestration.defs.ig_enriched.slv.prompts import prompt_identity, prompt_identity_v1


class _FakeAdapter:
    """Local test double implementing the ProviderAdapter Protocol."""

    def __init__(
        self,
        poll_sequence: list[str],
        results: list[Result] | None = None,
    ) -> None:
        self.name = "fake"
        self.model = "fake/model-1"
        self.capabilities = Capabilities(
            supports_async_batch=True,
            requires_tier_gate=False,
            supports_chunking=False,
            media_resolution="low",
        )
        self._polls = list(poll_sequence)
        self._results = results or []
        self.submitted: list[Item] = []
        self.retrieved_handle: str | None = None

    def build_request(self, item: Item) -> dict[str, object]:
        return {"prompt": item.prompt}

    def submit(self, items: list[Item], *, job_spec=DEFAULT_JOBSPEC) -> str:
        self.submitted = list(items)
        self.job_spec = job_spec
        return "handle-1"

    def poll(self, handle: str) -> str:
        if not self._polls:
            raise AssertionError("polled a fake with an exhausted poll sequence")
        return self._polls.pop(0)

    def normalize_state(self, raw: str) -> str:
        return raw

    def is_terminal(self, state: str) -> bool:
        return state in TERMINAL_STATES

    def retrieve(self, handle: str) -> list[Result]:
        self.retrieved_handle = handle
        return self._results

    def classify_error(self, exc: BaseException) -> str:
        return "terminal"


_ITEMS = [
    Item(custom_key="k1", prompt="p1", post_id="post-1", platform="instagram"),
    Item(custom_key="k2", prompt="p2", post_id="post-2", platform="instagram"),
]

_RESULTS = [
    Result(
        custom_key="k1",
        ok=True,
        response_text='{"topic": "devtools"}',
        error=None,
        post_id="post-1",
        platform="instagram",
        model="fake/model-1",
        provider="fake",
    ),
    Result(custom_key="k2", ok=False, response_text=None, error="boom"),
]


def test_canonical_states_exactly_four() -> None:
    assert CANONICAL_STATES == frozenset(
        {PENDING, PROCESSING, COMPLETED, FAILED}
    )


def test_terminal_states_are_completed_and_failed_only() -> None:
    assert TERMINAL_STATES == frozenset({COMPLETED, FAILED})
    assert PENDING not in TERMINAL_STATES
    assert PROCESSING not in TERMINAL_STATES


def test_unknown_adapter_raises_clear_error() -> None:
    with pytest.raises(KeyError, match="unknown adapter 'nope'"):
        build_adapter("nope")


def test_run_lifecycle_drives_submit_poll_retrieve() -> None:
    adapter = _FakeAdapter(
        [PENDING, PROCESSING, COMPLETED],
        list(_RESULTS),
    )
    observed, results = run_lifecycle(adapter, _ITEMS)
    assert observed == [PENDING, PROCESSING, COMPLETED]
    assert adapter.submitted == _ITEMS
    assert adapter.retrieved_handle == "handle-1"
    assert results == _RESULTS
    assert results[0].response_text == '{"topic": "devtools"}'


def test_run_lifecycle_fails_immediately() -> None:
    adapter = _FakeAdapter([FAILED], list(_RESULTS))
    observed, _ = run_lifecycle(adapter, _ITEMS)
    assert observed == [FAILED]


def test_run_lifecycle_respects_max_polls() -> None:
    adapter = _FakeAdapter([PROCESSING] * 100)
    with pytest.raises(TimeoutError, match="2 polls"):
        run_lifecycle(adapter, _ITEMS, max_polls=2)
    # no result retrieval after a timeout
    assert adapter.retrieved_handle is None


def test_run_lifecycle_rejects_non_canonical_state() -> None:
    adapter = _FakeAdapter(["weird"])
    with pytest.raises(AssertionError, match="non-canonical state"):
        run_lifecycle(adapter, _ITEMS)


def test_register_adapter_and_build() -> None:
    try:
        register_adapter("_test_fake", _FakeAdapter)
        adapter = build_adapter("_test_fake", poll_sequence=[COMPLETED])
        assert isinstance(adapter, _FakeAdapter)
        assert isinstance(adapter, ProviderAdapter)
    finally:
        del __import__("orchestration.defs.engine.provider", fromlist=["x"]).ADAPTER_REGISTRY[
            "_test_fake"
        ]


def test_prompt_identity_ignores_model() -> None:
    assert prompt_identity("prompt", "v3") == prompt_identity("prompt", "v3")
    assert prompt_identity("prompt", "v3") != prompt_identity("prompt", "v4")
    # schema_version is part of identity; the model is not an argument at all


def test_prompt_identity_v1_changes_with_model() -> None:
    assert prompt_identity_v1("prompt", "model-a") != prompt_identity_v1(
        "prompt", "model-b"
    )


def test_provider_error_status_code() -> None:
    err = ProviderError("bad request", status_code=400)
    assert err.status_code == 400
    assert ProviderError("no status").status_code is None
