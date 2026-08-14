"""Metadata-only re-scrape of tracked profiles → avatars.

Gets the tracked profile list from ``dim_profile``, triggers ONE batch
details-type Apify scrape, writes bronze, then runs ``ig_profiles_slv`` to
extract profile metadata and download avatars to ``data/media/avatars/``.

Usage:
    uv run python scripts/re_scrape_profiles.py [--limit N]
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

import polars as pl
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.apify import poll_run, stream_dataset, trigger_run
from datalake.defs.common.lake import BRONZE_LAKE, bronze_path
from datalake.defs.common.resources import ApifyResource, SQLiteResource
from datalake.defs.instagram.assets import ig_profiles_slv

ACTOR = "apify~instagram-scraper"


def _tracked_usernames(duckdb: DuckDBResource) -> list[str]:
    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT owner_username FROM dim_profile "
            "WHERE owner_username IS NOT NULL ORDER BY owner_username"
        ).fetchall()
    return [r[0] for r in rows]


def _scrape_details(urls: list[str], token: str) -> str:
    run = trigger_run(
        ACTOR,
        urls,
        token=token,
        results_limit=1,
        results_type="details",
    )
    dataset_id = poll_run(run.run_id, token=token)

    dest = bronze_path(dataset_id)
    if dest.exists():
        return dataset_id

    ndjson_path = BRONZE_LAKE / f"{dataset_id}.jsonl"
    item_count = stream_dataset(dataset_id, dest=ndjson_path, token=token)

    if item_count == 0:
        pl.DataFrame().write_parquet(dest)
    else:
        pl.read_ndjson(ndjson_path).write_parquet(dest)
    if ndjson_path.exists():
        ndjson_path.unlink()

    meta = {
        "run_id": run.run_id,
        "dataset_id": dataset_id,
        "actor": run.actor,
        "item_count": item_count,
        "input": {
            "urls": urls,
            "results_limit": 1,
            "results_type": "details",
        },
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    dest.with_suffix(".parquet.meta").write_text(json.dumps(meta, indent=2))
    print(f"  bronze: {dest.name} ({item_count} items)")
    return dataset_id


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="max profiles (0 = all)")
    args = parser.parse_args()

    apify = ApifyResource()
    if not apify.token:
        raise RuntimeError("APIFY_API_TOKEN not set")

    duckdb = DuckDBResource(database="data/state.duckdb")
    usernames = _tracked_usernames(duckdb)
    if args.limit:
        usernames = usernames[: args.limit]
    print(f"Re-scraping {len(usernames)} profile(s): {usernames}")

    urls = [f"https://www.instagram.com/{u}/" for u in usernames]
    _scrape_details(urls, token=apify.token)

    # Run ig_profiles_slv → silver_ig_profiles + avatars
    ops = SQLiteResource(database="data/ops.sqlite")
    ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
    result = ig_profiles_slv(ctx)
    print(f"Profiles extracted: {len(result)}")


if __name__ == "__main__":
    main()
