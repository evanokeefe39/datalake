"""Dagster resources — external dependencies injected into assets.

All env-token reads live here and nowhere else.
"""

from __future__ import annotations

import os

from dagster import ConfigurableResource
from dotenv import load_dotenv
from pydantic import Field

load_dotenv()


class ApifyResource(ConfigurableResource):
    """Apify API token. Assets that scrape use this resource."""

    token: str = Field(
        default_factory=lambda: os.environ.get("APIFY_API_TOKEN", ""),
        description="Apify API token.",
    )


class GeminiResource(ConfigurableResource):
    """Gemini API key + lazy client. Assets that enrich use this resource."""

    api_key: str = Field(
        default_factory=lambda: os.environ.get("GEMINI_API_KEY", ""),
        description="Gemini API key.",
    )
