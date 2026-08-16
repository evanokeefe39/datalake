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

    def analyze(
        self,
        prompt: str,
        *,
        model: str | None = None,
        media_resolution: str | None = None,
        count_tokens: bool = False,
        media_files: list[MediaFile] | None = None,
    ) -> str:
        """Send a prompt to Gemini and return the response text.

        Uses JSON mode, 0.2 temperature, and 2048 max output tokens.

        This method is a thin wrapper — it does NOT retry or back off.
        Rate-limit handling lives in the asset's retry loop (caller
        owns retry policy).

        Args:
            prompt: The full prompt text to send.
            model: Model name (default ``gemini-3.5-flash-lite``).
            media_resolution: ``"MEDIA_RESOLUTION_LOW"`` to reduce video frame
                token cost (66 vs 258 tokens/frame). Only relevant for video
                media. Defaults to ``"MEDIA_RESOLUTION_LOW"`` when
                ``media_files`` is provided.
            count_tokens: If True, do a pre-flight token count and raise
                if the prompt exceeds the safety limit before sending.
            media_files: Optional list of MediaFile dicts with ``uri`` and
                ``mime_type``. When provided, constructs multimodal contents
                with file Parts + text Part. When None, text-only path
                (unchanged behavior).

        Returns:
            Raw response text from Gemini. Caller is responsible for JSON
            parsing, retry handling, and rate-limit classification.

        Raises:
            ValueError: If ``count_tokens=True`` and the prompt exceeds
                the safety limit (1M tokens).
            google.genai.errors.APIError: On API errors (including 429 rate
                limits). Inspect ``exc.code`` (HTTP status), ``exc.status``
                (e.g. ``RESOURCE_EXHAUSTED``), and ``exc.message`` for
                subtype discrimination (rate_limit_exceeded vs
                insufficient_quota).
        """
        from google.genai import Client as GeminiClient
        from google.genai.types import GenerateContentConfig, Part

        model_name = model or _DEFAULT_GEMINI_MODEL

        if count_tokens:
            token_count = self.count_tokens(prompt, model=model_name)
            if token_count > _TOKEN_SAFETY_LIMIT:
                raise ValueError(
                    f"Prompt exceeds safety limit "
                    f"({token_count} > {_TOKEN_SAFETY_LIMIT} tokens) "
                    f"for model {model_name}"
                )

        client = GeminiClient(api_key=self.api_key)
        config_kwargs: dict = dict(
            response_mime_type="application/json",
            temperature=0.2,
            max_output_tokens=2048,
        )

        # Build contents: multimodal when media_files provided, text-only otherwise
        if media_files:
            if media_resolution is None:
                media_resolution = "MEDIA_RESOLUTION_LOW"
            config_kwargs["media_resolution"] = media_resolution

            contents: list[Part | str] = []
            for mf in media_files:
                contents.append(
                    Part.from_uri(
                        file_uri=mf["uri"],
                        mime_type=mf["mime_type"],
                    )
                )
            contents.append(Part.from_text(text=prompt))
        else:
            if media_resolution is not None:
                config_kwargs["media_resolution"] = media_resolution
            contents = prompt

        response = client.models.generate_content(
            model=model_name,
            contents=contents,
            config=GenerateContentConfig(**config_kwargs),
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
