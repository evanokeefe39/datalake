"""Dagster resources — external dependencies injected into assets.

All env-token reads live here and nowhere else.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import TypedDict

import polars as pl
from dagster import ConfigurableIOManager, ConfigurableResource
from dagster_duckdb import DuckDBResource  # noqa: F401 — re-exported
from dotenv import load_dotenv
from pydantic import Field

load_dotenv()
_DEFAULT_OPS_DB = "data/ops.sqlite"



class MediaFile(TypedDict):
    """A media file reference for multimodal Gemini analysis.

    ``uri`` is a Gemini File API URI (``files/…`` or full URL).
    ``mime_type`` is the IANA media type (e.g. ``"video/mp4"``, ``"image/jpeg"``).
    """

    uri: str
    mime_type: str

class SQLiteResource(ConfigurableResource):
    """SQLite database resource for operational state (queue, media cache, dead_letter).

    Thin wrapper around sqlite3 with WAL mode enabled on connection.
    Mirrors DuckDBResource pattern — ``database`` path + ``get_connection()``.
    """

    database: str = Field(
        default_factory=lambda: os.environ.get("OPS_DB_PATH", _DEFAULT_OPS_DB),
        description="Path to the operational SQLite database.",
    )

    def get_connection(self) -> sqlite3.Connection:
        """Return a sqlite3 connection with WAL mode, foreign keys, and optimized pragmas.

        The caller owns the connection lifecycle — close when done.
        """
        conn = sqlite3.connect(self.database)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn


class ApifyResource(ConfigurableResource):
    """Apify API token. Assets that scrape use this resource."""

    token: str = Field(
        default_factory=lambda: os.environ.get("APIFY_API_TOKEN", ""),
        description="Apify API token.",
    )


_DEFAULT_GEMINI_MODEL = "gemini-3.5-flash-lite"
"""Default model — flash-lite family, lowest cost for high-volume enrichment."""

_TOKEN_SAFETY_LIMIT = 1_000_000
"""Max input tokens before count_tokens pre-check raises (safety net)."""


class GeminiResource(ConfigurableResource):
    """Gemini API key + lazy client. Assets that enrich use this resource."""

    api_key: str = Field(
        default_factory=lambda: os.environ.get("GEMINI_API_KEY", ""),
        description="Gemini API key.",
    )

    def count_tokens(
        self,
        prompt: str,
        *,
        model: str | None = None,
    ) -> int:
        """Count tokens in a prompt without generating.

        Args:
            prompt: The prompt text to count.
            model: Model to count against (default ``_DEFAULT_MODEL``).

        Returns:
            Total token count for the prompt.
        """
        from google.genai import Client as GeminiClient

        client = GeminiClient(api_key=self.api_key)
        response = client.models.count_tokens(
            model=model or _DEFAULT_GEMINI_MODEL,
            contents=prompt,
        )
        return response.total_tokens


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


