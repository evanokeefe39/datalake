"""Tests for the ``ig_posts_raw`` bronze asset.

Gap-fills per test-hardening plan:
- Schema validation, row count, .meta integrity, run_id, partial data, list columns
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_context

from datalake.defs.instagram.assets import ig_posts_raw
from datalake.defs.instagram.config import ScrapeConfig



class _FakeRunInfo:
    """Mimics ig_pipeline.models.RunInfo."""

    def __init__(self, run_id: str = "run_1", dataset_id: str = "ds_1"):
        self.run_id = run_id
        self.default_dataset_id = dataset_id
        self.actor = "apify~instagram-scraper"
        self.estimated_cost_usd = 0.0


class _FakeApifyResource:
    """Minimal ApifyResource stand-in for test injection."""

    def __init__(self, token: str = "fake-token"):
        self.token = token


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def mock_apify_success():
    """Mock Apify functions to simulate a successful scrape."""
    def _mock_stream(dataset_id, dest, *, token):
        lines = [
            '{"id":"1","shortCode":"abc","caption":"Post one","ownerUsername":"user1",'
            '"likesCount":10,"commentsCount":2,"timestamp":"2024-01-01T00:00:00.000Z"}',
            '{"id":"2","shortCode":"def","caption":"Post two","ownerUsername":"user2",'
            '"likesCount":20,"commentsCount":3,"timestamp":"2024-01-02T00:00:00.000Z"}',
        ]
        dest.write_text("\n".join(lines))
        return len(lines)

    with (
        patch("datalake.defs.instagram.assets.trigger_run",
              return_value=_FakeRunInfo("run_1", "ds_1")),
        patch("datalake.defs.instagram.assets.poll_run",
              return_value="ds_1"),
        patch("datalake.defs.instagram.assets.stream_dataset",
              side_effect=_mock_stream),
    ):
        yield

@pytest.fixture
def mock_apify_failed():
    """Mock poll to return FAILED status."""
    with (
        patch("datalake.defs.instagram.assets.trigger_run",
              return_value=_FakeRunInfo("run_fail", "ds_fail")),
        patch("datalake.defs.instagram.assets.poll_run",
              side_effect=RuntimeError("Run FAILED: actor crashed")),
    ):
        yield


@pytest.fixture
def mock_apify_timeout():
    """Mock poll to raise timeout."""
    with (
        patch("datalake.defs.instagram.assets.trigger_run",
              return_value=_FakeRunInfo("run_timeout", "ds_timeout")),
        patch("datalake.defs.instagram.assets.poll_run",
              side_effect=RuntimeError("Run timed out after 600s")),
    ):
        yield


@pytest.fixture
def mock_apify_empty():
    """Mock stream_dataset to write 0 items."""

    def _mock_empty(dataset_id, dest, *, token):
        dest.write_text("")
        return 0

    with (
        patch("datalake.defs.instagram.assets.trigger_run",
              return_value=_FakeRunInfo("run_empty", "ds_empty")),
        patch("datalake.defs.instagram.assets.poll_run",
              return_value="ds_empty"),
        patch("datalake.defs.instagram.assets.stream_dataset",
              side_effect=_mock_empty),
    ):
        yield


# ── Tests ──────────────────────────────────────────────────────────────────


def test_successful_scrape(mock_apify_success, tmp_path):
    """GIVEN valid config + Apify resource
    WHEN ig_posts_raw executes
    THEN Parquet written with correct rows AND .meta sidecar exists
    AND asset returns pl.DataFrame
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.bronze_path") as mbp:
            dest = tmp_path / "ds_1.parquet"
            mbp.return_value = dest

            context = build_asset_context()
            config = ScrapeConfig(
                urls=["https://instagram.com/test"],
                results_limit=2,
            )
            result = ig_posts_raw(
                context, config=config, apify=_FakeApifyResource(),
            )

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 2
            assert dest.exists()

            df = pl.read_parquet(dest)
            assert len(df) == 2
            assert set(df["shortCode"].to_list()) == {"abc", "def"}

            meta_path = dest.with_suffix(".parquet.meta")
            assert meta_path.exists()
            meta = json.loads(meta_path.read_text())
            assert meta["run_id"] == "run_1"
            assert meta["actor"] == "apify~instagram-scraper"
            assert meta["item_count"] == 2
            assert "downloaded_at" in meta


def test_idempotent_rerun(mock_apify_success, tmp_path):
    """GIVEN Parquet file already exists
    WHEN ig_posts_raw runs again
    THEN it returns existing data (no re-download)
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.bronze_path") as mbp:
            dest = tmp_path / "ds_1.parquet"
            mbp.return_value = dest

            existing = pl.DataFrame(
                {"shortCode": ["pre_existing"], "id": ["0"]},
            )
            existing.write_parquet(dest)

            context = build_asset_context()
            config = ScrapeConfig(
                urls=["https://instagram.com/test"],
                results_limit=2,
            )
            result = ig_posts_raw(
                context, config=config, apify=_FakeApifyResource(),
            )

            assert result["shortCode"].to_list() == ["pre_existing"]


def test_empty_dataset(mock_apify_empty, tmp_path):
    """GIVEN Apify returns 0 items
    WHEN ig_posts_raw executes
    THEN Parquet with 0 rows (not an error)
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.bronze_path") as mbp:
            dest = tmp_path / "ds_empty.parquet"
            mbp.return_value = dest

            context = build_asset_context()
            config = ScrapeConfig(
                urls=["https://instagram.com/test"],
                results_limit=0,
            )
            result = ig_posts_raw(
                context, config=config, apify=_FakeApifyResource(),
            )

            assert isinstance(result, pl.DataFrame)
            assert result.is_empty()
            assert dest.exists()
            assert pl.read_parquet(dest).is_empty()


def test_apify_failure_raises(mock_apify_failed, tmp_path):
    """GIVEN Apify run fails
    WHEN ig_posts_raw polls
    THEN RuntimeError with failure message
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.bronze_path") as mbp:
            mbp.return_value = tmp_path / "ds_fail.parquet"

            context = build_asset_context()
            config = ScrapeConfig(
                urls=["https://instagram.com/test"],
                results_limit=2,
            )
            with pytest.raises(RuntimeError, match="FAILED"):
                ig_posts_raw(
                    context, config=config,
                    apify=_FakeApifyResource(),
                )


def test_apify_timeout_raises(mock_apify_timeout, tmp_path):
    """GIVEN Apify run times out
    WHEN ig_posts_raw polls
    THEN RuntimeError indicating timeout
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.bronze_path") as mbp:
            mbp.return_value = tmp_path / "ds_timeout.parquet"

            context = build_asset_context()
            config = ScrapeConfig(
                urls=["https://instagram.com/test"],
                results_limit=2,
            )
            with pytest.raises(RuntimeError, match="timed out"):
                ig_posts_raw(
                    context, config=config,
                    apify=_FakeApifyResource(),
                )


def test_missing_token_raises(tmp_path):
    """GIVEN ApifyResource.token is empty
    WHEN ig_posts_raw executes
    THEN RuntimeError before any API call
    """
    context = build_asset_context()
    config = ScrapeConfig(
        urls=["https://instagram.com/test"],
        results_limit=2,
    )
    with pytest.raises(RuntimeError, match="token is empty"):
        ig_posts_raw(
            context, config=config,
            apify=_FakeApifyResource(token=""),
        )
