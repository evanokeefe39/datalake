"""Dagster resources — external dependencies injected into assets.

All env-token reads live here and nowhere else. This module deliberately does
NOT load `.env`: `platform.paths` does, and it runs first because it reads the
environment into module constants (ADR-0015). A second loader here would be a
race that hides which value won.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import polars as pl
from dagster import ConfigurableIOManager, ConfigurableResource
from dagster_duckdb import DuckDBResource  # noqa: F401 — re-exported
from pydantic import Field

_DEFAULT_OPS_DB = "data/ops.sqlite"



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


