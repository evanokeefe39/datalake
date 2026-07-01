"""Rebuild state.duckdb from raw bronze Parquet files.

Drops all tables/views, runs silver → gold → serving assets in sequence.
Requires GEMINI_API_KEY in env for gold enrichment.
"""

import os

from dagster import build_asset_context

from datalake.defs.common.resources import DuckDBResource, GeminiResource
from datalake.defs.instagram.assets import ig_posts_gld, ig_posts_slv
from datalake.defs.serving.assets import analytics_views, profile_dimension


DB_PATH = "data/state.duckdb"


def drop_everything():
    import duckdb

    db = duckdb.connect(DB_PATH)
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main'"
    ).fetchall()
    views = db.execute(
        "SELECT table_name FROM information_schema.views "
        "WHERE table_schema = 'main'"
    ).fetchall()
    for (t,) in tables:
        db.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
        print(f"  DROPPED TABLE {t}")
    for (v,) in views:
        db.execute(f'DROP VIEW IF EXISTS "{v}"')
        print(f"  DROPPED VIEW {v}")
    db.close()


def main():
    print("=== Step 0: Drop all existing tables/views ===")
    drop_everything()

    duckdb = DuckDBResource(database=DB_PATH)
    gemini = GeminiResource()
    has_api_key = bool(os.environ.get("GEMINI_API_KEY"))

    print("\n=== Step 1: Silver (ig_posts_slv) ===")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    silver = ig_posts_slv(ctx)
    print(f"  Result: {len(silver)} rows, {silver['post_id'].n_unique()} unique post_ids")

    if has_api_key:
        print("\n=== Step 2: Gold (ig_posts_gld) ===")
        ctx = build_asset_context(resources={"duckdb": duckdb, "gemini": gemini})
        gold = ig_posts_gld(ctx)
        print(f"  Result: {len(gold)} rows enriched")
    else:
        print("\n=== Step 2: Gold SKIPPED (no GEMINI_API_KEY) ===")

    print("\n=== Step 3: Serving (profile_dimension + analytics_views) ===")
    ctx = build_asset_context(resources={"duckdb": duckdb})
    profile_dimension(ctx)
    analytics_views(ctx)
    print("  Done.")

    print("\n=== Step 4: Verify ===")
    import duckdb as ddb

    db = ddb.connect(DB_PATH, read_only=True)
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    for (t,) in tables:
        cnt = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t:30s} {cnt} rows")

    wm = db.execute("SELECT name, timestamp FROM watermarks ORDER BY name").fetchall()
    if wm:
        print("\n  Watermarks:")
        for n, ts in wm:
            print(f"    {n:25s} {ts}")
    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
