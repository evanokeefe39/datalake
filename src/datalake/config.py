"""Dagster Config schemas — typed, validated, surfaced in the launchpad."""

from __future__ import annotations

from dagster import Config


class ScrapeConfig(Config):
    """Configuration for triggering an Apify Instagram scrape."""

    urls: list[str]
    results_limit: int = 12
    results_type: str = "posts"


class GoldConfig(Config):
    """Configuration for the ``gold_analyses`` asset.

    ``post_ids`` (optional) restricts enrichment to specific posts.
    Default (empty) = all pending posts.
    """

    post_ids: list[str] = []
