"""Unit tests for the verbatim bronze landing (`bronze_enrichment_raw`).

Contract under test: landing is verbatim (byte-identical `response_text`),
immutable + append-only, idempotent by the natural key
`(post_id, platform, workload, prompt_hash, run_id)`, carries an explicit
`ok` column (failure is READ, never inferred), and never touches the real
`data/lake/` — every test uses `tmp_path` as the lake root.
"""

from __future__ import annotations

import pytest
from orchestration.defs.engine.landing import (
    DATASET_ID,
    KEY_COLUMNS,
    SCHEMA,
    WORKLOAD_CONTENT_CLASSIFICATION,
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    WORKLOADS,
    land_response,
    read_responses,
    response_path,
)

VERBATIM_TEXT = (
    '{"facets": [\n'
    '\t{"name": "face_present", "value": "yes", "note": "quote: \\"she said\\"",\n'
    '  "unicode": "café — 中文 \U0001F525 \U0001F4F7", "tabs": "\\t}"\n'
    '\t}\n'
    ']}'
)


@pytest.fixture
def root(tmp_path):
    return tmp_path / "lake" / "bronze"


def _land(root, **overrides):
    kwargs = {
        "post_id": "1",
        "platform": "instagram",
        "workload": WORKLOAD_GROWTH_FACETS_VISUAL,
        "provider": "qwen",
        "model": "qwen/qwen3.7-flash",
        "prompt_hash": "abc123",
        "schema_version": "3",
        "run_id": "run-1",
        "response_text": VERBATIM_TEXT,
        "ok": True,
    }
    kwargs.update(overrides)
    return land_response(root=root, **kwargs)


# ── Verbatim round-trip ────────────────────────────────────────────────────


def test_round_trip_verbatim_byte_identical(root):
    assert _land(root) == 1
    df = read_responses(root)
    assert df.height == 1
    landed = df["response_text"][0]
    # Codepoint-identical: embedded newlines, tabs, unicode, and quotes all
    # survive the Parquet round-trip unparsed.
    assert landed == VERBATIM_TEXT
    assert df["ok"][0] is True


def test_round_trip_preserves_unicode_and_quotes(root):
    text = 'line1\n"line2" with unicode: café 中文 \U0001F525\n\tline3 with \\"nested\\"'
    _land(root, response_text=text, prompt_hash="u1")
    assert read_responses(root)["response_text"][0] == text


def test_read_responses_empty_is_typed(root):
    df = read_responses(root)
    assert df.height == 0
    assert df.schema == SCHEMA
    assert set(KEY_COLUMNS).issubset(df.columns)


# ── Idempotency + append-only by natural key ───────────────────────────────


def test_same_key_lands_exactly_one_row(root):
    assert _land(root) == 1
    # Re-landing the identical five-part key is a no-op, not a duplicate.
    assert _land(root, response_text="different text, same key") == 0
    df = read_responses(root)
    assert df.height == 1
    # The first-landed verbatim row is untouched.
    assert df["response_text"][0] == VERBATIM_TEXT


def test_different_prompt_hash_same_post_lands_two_rows(root):
    assert _land(root, prompt_hash="h1") == 1
    assert _land(root, prompt_hash="h2") == 1
    df = read_responses(root)
    assert df.height == 2
    assert sorted(df["prompt_hash"].to_list()) == ["h1", "h2"]


def test_five_part_key_enforced_each_component(root):
    """Differing in ANY of the five key components is a new row, not an
    overwrite; sharing all five is a no-op."""
    assert _land(root) == 1
    variants = {
        "post_id": "2",
        "platform": "tiktok",
        "workload": WORKLOAD_GROWTH_FACETS_TEXT,
        "prompt_hash": "other",
        "run_id": "run-2",
    }
    for col, value in variants.items():
        assert _land(root, **{col: value}) == 1, f"differing {col} must land"
    df = read_responses(root)
    assert df.height == 6  # 1 original + 5 single-component variants
    assert df.n_unique(subset=list(KEY_COLUMNS)) == 6
    # Append-only: the original row still carries its verbatim text.
    original = df.filter(
        (df["post_id"] == "1")
        & (df["platform"] == "instagram")
        & (df["workload"] == WORKLOAD_GROWTH_FACETS_VISUAL)
        & (df["prompt_hash"] == "abc123")
        & (df["run_id"] == "run-1")
    )
    assert original["response_text"][0] == VERBATIM_TEXT


# ── Explicit ok column (S5) ────────────────────────────────────────────────


def test_ok_column_distinguishes_failure_from_success(root):
    assert _land(root, prompt_hash="ok-1") == 1
    assert (
        _land(
            root,
            prompt_hash="fail-1",
            ok=False,
            response_text="provider error body",
            error_message="terminal 404",
        )
        == 1
    )
    df = read_responses(root)
    assert "ok" in df.columns
    # Read the column ALONE — never infer status from a missing row.
    success = df.filter(df["prompt_hash"] == "ok-1")
    failure = df.filter(df["prompt_hash"] == "fail-1")
    assert success["ok"].to_list() == [True]
    assert failure["ok"].to_list() == [False]
    # Both rows are landed: the failure record exists, is verbatim, and
    # carries the reason.
    assert df.height == 2
    assert failure["response_text"][0] == "provider error body"
    assert failure["error_message"][0] == "terminal 404"


def test_failed_landing_requires_error_message(root):
    with pytest.raises(ValueError, match="error_message"):
        _land(root, prompt_hash="bad", ok=False)


def test_provenance_columns_present(root):
    _land(
        root,
        request_echo_json='{"items": 1}',
        input_modality="frames",
        sampling_params_json='{"temperature": 0}',
    )
    df = read_responses(root)
    row = df.row(0, named=True)
    assert row["provider"] == "qwen"
    assert row["model"] == "qwen/qwen3.7-flash"
    assert row["schema_version"] == "3"
    assert row["run_id"] == "run-1"
    assert row["landing_at"] is not None
    assert row["analysed_at"] is not None
    assert row["request_echo_json"] == '{"items": 1}'
    assert row["input_modality"] == "frames"
    assert row["sampling_params_json"] == '{"temperature": 0}'


# ── Workload discipline ────────────────────────────────────────────────────


def test_unknown_workload_raises(root):
    with pytest.raises(ValueError, match="unknown workload"):
        _land(root, workload="qwen-vision")  # doc string, NOT a constant


def test_workload_constants_are_named_members():
    assert WORKLOAD_GROWTH_FACETS_VISUAL == "growth-facets-visual"
    assert WORKLOAD_GROWTH_FACETS_TEXT == "growth-facets-text"
    assert WORKLOAD_CONTENT_CLASSIFICATION == "content-classification"
    assert WORKLOADS == frozenset(
        {
            WORKLOAD_GROWTH_FACETS_VISUAL,
            WORKLOAD_GROWTH_FACETS_TEXT,
            WORKLOAD_CONTENT_CLASSIFICATION,
        }
    )


def test_each_named_workload_round_trips(root):
    for workload in (WORKLOAD_GROWTH_FACETS_VISUAL, WORKLOAD_GROWTH_FACETS_TEXT,
                     WORKLOAD_CONTENT_CLASSIFICATION):
        assert _land(root, workload=workload, prompt_hash=workload) == 1
    df = read_responses(root)
    assert set(df["workload"].to_list()) == set(WORKLOADS)


# ── Path conventions (lake.py house pattern) ───────────────────────────────


def test_response_path_follows_lake_convention(root):
    path = response_path(root)
    assert path == root / f"{DATASET_ID}.parquet"
    assert path.parent == root


def test_default_root_is_bronze_lake():
    # Import-level default routes to the bronze lake root (env-overridable
    # by lake.py) — exercised here only for the convention, not the write.
    from orchestration.defs.platform import paths as lake

    assert response_path(None) == lake.BRONZE_LAKE / f"{DATASET_ID}.parquet"


def test_every_registered_adapter_may_land_in_the_live_root():
    """REGRESSION: the landing allowlist drifted from the adapter registry.

    ``land_response(root=None)`` guards the LIVE bronze lake by refusing any
    provider not in ``KNOWN_PROVIDERS``. When the Gemini path was retired the
    surviving adapter was renamed to ``service_backed`` — named for the SEAM,
    not the vendor — but the allowlist kept the old vendor names. Every harvest
    was therefore refused at landing while submit, poll, the graph validation
    and the whole suite stayed green: the failure only existed on the paid path.

    This pins the two definitions together. A future adapter rename or addition
    now fails HERE, in CI, instead of in production.
    """
    # Importing the adapter module performs its registration side effect.
    from orchestration.defs.engine import (
        provider,
        service_backed,  # noqa: F401
    )
    from orchestration.defs.engine.landing import KNOWN_PROVIDERS

    registered = set(provider.ADAPTER_REGISTRY)
    assert registered, "no adapters registered — the registry contract is broken"

    missing = registered - KNOWN_PROVIDERS
    assert not missing, (
        f"registered adapter(s) {sorted(missing)} are absent from "
        f"KNOWN_PROVIDERS {sorted(KNOWN_PROVIDERS)} — every harvest from them "
        "would be refused at landing with root=None (the live lake). Add the "
        "adapter's registered name to KNOWN_PROVIDERS alongside its producer."
    )
