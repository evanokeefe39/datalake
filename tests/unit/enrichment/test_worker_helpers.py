"""Unit tests for the enrichment worker's rate-limit helpers.

These cover the two failure modes found in the architecture review:

1. Every 429 was classified as daily-quota exhaustion because ``_QUOTA_KEYWORDS``
   included ``"429"`` / ``"rate limit"`` — a burst 429 halted the whole batch
   until midnight instead of retrying.
2. ``_exponential_backoff`` returned ``int(2**N + uniform(0,1))``, which floors
   the jitter away — "jittered backoff" was deterministic.
"""

from __future__ import annotations

from scripts.enrichment_worker import (
    _exponential_backoff,
    _is_quota_exhausted,
    _is_rate_limited,
)


class _FakeError(Exception):
    """An exception carrying a Gemini-style ``details`` attribute."""

    def __init__(self, details: str = ""):
        super().__init__()
        self.details = details


def test_is_quota_exhausted_matches_quota_keywords():
    exc = _FakeError()
    assert _is_quota_exhausted(exc, "insufficient quota for this project")
    assert _is_quota_exhausted(exc, "you have exhausted your daily limit")


def test_is_quota_exhausted_ignores_burst_429():
    """A burst 429 must NOT be classified as quota exhaustion."""
    exc = _FakeError()
    assert not _is_quota_exhausted(exc, "429 RESOURCE_EXHAUSTED: rate limit exceeded")
    assert not _is_quota_exhausted(exc, "429 Too Many Requests")


def test_is_quota_exhausted_reads_structured_details():
    """The API's structured ``insufficient_quota`` marker wins."""
    exc = _FakeError(details="insufficient_quota")
    assert _is_quota_exhausted(exc, "429 RESOURCE_EXHAUSTED")


def test_is_rate_limited_matches_burst_429():
    exc = _FakeError()
    assert _is_rate_limited(exc, "429 RESOURCE_EXHAUSTED: rate limit exceeded")
    assert _is_rate_limited(exc, "429 Too Many Requests")


def test_is_rate_limited_ignores_quota():
    exc = _FakeError()
    assert not _is_rate_limited(exc, "insufficient quota for this project")


def test_exponential_backoff_has_real_jitter():
    samples = {_exponential_backoff(1) for _ in range(20)}
    # Real jitter (float) yields multiple distinct values; the truncated
    # int() version would collapse to a single value.
    assert len(samples) > 1
    assert all(isinstance(s, float) for s in samples)
