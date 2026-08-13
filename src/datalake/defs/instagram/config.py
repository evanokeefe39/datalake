"""Dagster Config schemas — typed, validated, surfaced in the launchpad."""

from __future__ import annotations

import os
from enum import Enum

from dagster import Config


class ResultsType(str, Enum):
    """Valid ``resultsType`` values for the Apify Instagram scraper."""

    POSTS = "posts"
    DETAILS = "details"
    COMMENTS = "comments"


class ScrapeConfig(Config):
    """Configuration for triggering an Apify Instagram scrape."""

    urls: list[str]
    results_limit: int = 12
    results_type: ResultsType = ResultsType.POSTS


class GoldConfig(Config):
    """Configuration for ``ig_posts_gen_batches`` (gold batch creation).

    ``post_ids`` (optional) restricts enrichment to specific posts.
    Default (empty) = all pending posts.
    """

    post_ids: list[str] = []


# ── Gemini Tier Configuration ──────────────────────────────────────────────



class GeminiTier(Enum):
    """Gemini API access tier — controls rate limits and feature availability."""

    FREE = "free"
    TIER_1 = "tier1"
    TIER_2 = "tier2"
    """Tier 2 enables batch + larger batch jobs. Video enrichment is gated here."""

    @classmethod
    def detect(cls) -> GeminiTier:
        """Detect tier from ``GEMINI_TIER`` env var. Defaults to FREE."""
        raw = os.environ.get("GEMINI_TIER", "free").strip().lower()
        for tier in cls:
            if tier.value == raw:
                return tier
        return cls.FREE


class GeminiTierConfig:
    """Per-tier limits derived from the active Gemini tier.

    Detects tier from ``GEMINI_TIER`` env var (free/tier1/tier2) and
    exposes limits for the enrich assets.

    Usage::

        cfg = GeminiTierConfig.detect()
        if cfg.supports_batch:
            ...
    """

    tier: GeminiTier

    def __init__(self, tier: GeminiTier | None = None) -> None:
        self.tier = tier or GeminiTier.detect()

    @classmethod
    def detect(cls) -> GeminiTierConfig:
        return cls()

    @property
    def max_posts_per_run(self) -> int:
        """Max posts to process in one interactive enrichment run."""
        if self.tier == GeminiTier.FREE:
            return 10
        return 0  # 0 = unlimited

    @property
    def supports_video(self) -> bool:
        """True if video processing is available (Tier 1+)."""
        return self.tier in (GeminiTier.TIER_1, GeminiTier.TIER_2)

    @property
    def supports_batch(self) -> bool:
        """True if batch API is available (Tier 1+)."""
        return self.tier in (GeminiTier.TIER_1, GeminiTier.TIER_2)

    @property
    def max_batch_tokens(self) -> int:
        """Max total tokens per batch job submission."""
        if self.tier == GeminiTier.TIER_1:
            return 10_000_000
        if self.tier == GeminiTier.TIER_2:
            return 128_000_000
        return 0  # batch not supported on free tier

    @property
    def default_rpm(self) -> int:
        """Default RPM limit for paced processing (conservative)."""
        if self.tier == GeminiTier.FREE:
            return 10
        if self.tier == GeminiTier.TIER_1:
            return 30
        return 60
