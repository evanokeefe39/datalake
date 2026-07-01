"""Dagster resources — external dependencies injected into assets.

All env-token reads live here and nowhere else.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
from dagster import ConfigurableIOManager, ConfigurableResource
from dagster_duckdb import DuckDBResource  # noqa: F401 — re-exported
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

    def analyze(self, prompt: str) -> str:
        """Send a prompt to Gemini and return the response text.

        Uses the gold asset's standard model config (gemini-2.0-flash-lite,
        JSON mode, 0.2 temperature, 2048 max tokens).

        Args:
            prompt: The full prompt text to send.

        Returns:
            Raw response text from Gemini. Caller is responsible for JSON
            parsing and retry handling.

        Raises:
            RuntimeError: On API failure after exhausting retries.
        """
        from google.genai import Client as GeminiClient
        from google.genai.types import GenerateContentConfig

        client = GeminiClient(api_key=self.api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash-lite",
            contents=prompt,
            config=GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.2,
                max_output_tokens=2048,
            ),
        )
        return response.text


class PolarsIOManager(ConfigurableIOManager):
    """Polars-based I/O manager for Parquet persistence.

    Used by silver/gold assets for deterministic output paths.
    Bronze asset bypasses this (dynamic dataset_id paths).
    """

    lake_root: str = "data/lake"

    def _get_path(self, context) -> str:
        return str(Path(self.lake_root) / f"{context.asset_key.path[-1]}.parquet")

    def handle_output(self, context, obj: pl.DataFrame) -> None:
        path = self._get_path(context)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        obj.write_parquet(path)

    def load_input(self, context) -> pl.DataFrame:
        path = self._get_path(context)
        if not Path(path).exists():
            raise FileNotFoundError(f"Input Parquet not found: {path}")
        return pl.read_parquet(path)
