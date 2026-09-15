"""Prompt-identity contract (US-EENG-3 phase 3).

`compute_prompt_hash` must be a pure function of (prompt, output schema
version) — never the model, provider, or tier — so switching providers does
not fork hashes and mark the corpus stale. `legacy_prompt_hash` reproduces the
OLD model-bound hash so existing `gold_analyses` rows written before the
migration can be identified (the migration is versioned/verified, never
silent — no row is rewritten here).
"""

from __future__ import annotations

import pytest

from datalake.defs.enrichment.prompts import (
    IG_GOLD_PROMPT,
    IG_GOLD_SCHEMA_VERSION,
    compute_prompt_hash,
    legacy_prompt_hash,
)
from datalake.defs.enrichment.seam import prompt_identity, prompt_identity_v1

_MODELS = ("gemini-3.5-flash-lite", "qwen/qwen3.7-flash")
_PROVIDERS = ("gemini", "qwen-batch")
_PROMPT = IG_GOLD_PROMPT


# ── Core contract: identity binds prompt + schema version ONLY ──────────────


def test_same_prompt_schema_same_hash_across_models_and_providers() -> None:
    """THE point of the change: model and provider must NOT fork the hash.

    Column-collision check, asserted as two separate explicit invariants:
    (a) the hash does NOT vary with the model, (b) it DOES vary with
    schema_version.
    """
    # (a) model-independence — across two models AND two providers
    h_a = compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION)
    for model in _MODELS:
        for provider in _PROVIDERS:
            # the hash function's signature takes no model/provider argument;
            # simulate the "same analysis executed elsewhere" by asserting the
            # identity is reproducible and that model-bound legacy hashes DIFFER
            assert compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION) == h_a
            # provenance stays beside the result; it must not leak into identity
            assert compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION) != (
                legacy_prompt_hash(_PROMPT, model)
            )
    assert h_a == prompt_identity(_PROMPT, IG_GOLD_SCHEMA_VERSION)


def test_hash_varies_with_schema_version() -> None:
    """Explicit: a changed schema_version CHANGES the hash (re-hash on schema)."""
    h1 = compute_prompt_hash(_PROMPT, "1")
    h2 = compute_prompt_hash(_PROMPT, "2")
    assert h1 != h2
    # every distinct version gets a distinct identity
    assert len({compute_prompt_hash(_PROMPT, str(v)) for v in range(1, 6)}) == 5


def test_hash_varies_with_prompt() -> None:
    """A changed prompt CHANGES the hash (staleness detection still works)."""
    h1 = compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION)
    h2 = compute_prompt_hash(_PROMPT + "\nOne extra instruction.", IG_GOLD_SCHEMA_VERSION)
    assert h1 != h2
    # whitespace-only changes are content changes too (verbatim prompt text)
    assert h1 != compute_prompt_hash(_PROMPT + " ", IG_GOLD_SCHEMA_VERSION)


def test_column_collision_independence() -> None:
    """Two separate explicit assertions: model-independence, schema-dependence.

    Simulates the gold-table column layout: the same logical analysis executed
    under different `model` column values must carry the SAME prompt_hash, and
    the same prompt under a bumped schema must carry a DIFFERENT one.
    """
    model_a, model_b = _MODELS

    # explicit assertion 1: hash does NOT vary with model
    row_a = {"prompt_hash": compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION),
             "model": model_a}
    row_b = {"prompt_hash": compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION),
             "model": model_b}
    assert row_a["prompt_hash"] == row_b["prompt_hash"]

    # explicit assertion 2: hash DOES vary with schema_version
    row_v1 = {"prompt_hash": compute_prompt_hash(_PROMPT, "1"), "schema": "1"}
    row_v2 = {"prompt_hash": compute_prompt_hash(_PROMPT, "2"), "schema": "2"}
    assert row_v1["prompt_hash"] != row_v2["prompt_hash"]


# ── Migration: legacy rows are identifiable, never silently rewritten ───────


def test_legacy_hash_reproduces_old_model_bound_hash() -> None:
    """`legacy_prompt_hash` must reproduce the pre-migration scheme exactly.

    Existing `gold_analyses` rows carry
    `sha256(f"{prompt}:{model}")[:16]`; identification of those rows is the
    documented reconciliation step — compute here, compare there, rewrite
    nowhere.
    """
    import hashlib

    prompt, model = _PROMPT, _MODELS[0]
    expected = hashlib.sha256(f"{prompt}:{model}".encode()).hexdigest()[:16]
    assert legacy_prompt_hash(prompt, model) == expected
    assert legacy_prompt_hash(prompt, model) == prompt_identity_v1(prompt, model)
    # the old scheme WAS model-bound (this is what forked hashes across
    # providers): different models -> different legacy hashes for the same prompt
    assert legacy_prompt_hash(prompt, _MODELS[0]) != legacy_prompt_hash(
        prompt, _MODELS[1]
    )


def test_new_hash_differs_from_legacy_for_same_prompt() -> None:
    """The migration is real and detectable: new vs legacy hashes differ.

    This is what makes pre/post-migration rows distinguishable in
    `gold_analyses.prompt_hash` — and what keeps the reconciliation an
    auditable, verified step instead of a silent rewrite.
    """
    for model in _MODELS:
        assert compute_prompt_hash(_PROMPT, IG_GOLD_SCHEMA_VERSION) != (
            legacy_prompt_hash(_PROMPT, model)
        )
    # and the difference is attributable to the scheme, not the version string:
    # even the same version label under the legacy separator convention differs
    assert compute_prompt_hash(_PROMPT, "1") != legacy_prompt_hash(_PROMPT, "1")


def test_current_prompt_hash_uses_new_scheme() -> None:
    """The module-level constant is computed under the new (model-free) scheme."""
    from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

    assert CURRENT_PROMPT_HASH == compute_prompt_hash(
        _PROMPT, IG_GOLD_SCHEMA_VERSION
    )
    for model in _MODELS:
        assert CURRENT_PROMPT_HASH != legacy_prompt_hash(_PROMPT, model)


# ── Error paths ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("bad", [None, ""])
def test_empty_prompt_rejected(bad: str | None) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        compute_prompt_hash(bad, IG_GOLD_SCHEMA_VERSION)
    with pytest.raises(ValueError, match="non-empty"):
        legacy_prompt_hash(bad, _MODELS[0])


@pytest.mark.parametrize("bad", [None, ""])
def test_empty_schema_version_rejected(bad: str | None) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        compute_prompt_hash(_PROMPT, bad)
