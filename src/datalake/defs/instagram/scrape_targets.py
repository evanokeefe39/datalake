"""``scrape_targets`` control table — source of truth for tracked profiles.

The dashboard CRUD page and the datalake's profile ingestion both read and
write this table in ``ops.sqlite``. Column schema mirrors
``common.schemas.SQLITE_TABLES``.

``username`` is the primary key and the human-facing identifier.
``profile_url`` is the full Instagram URL fed to the Apify actor.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..common.resources import SQLiteResource


def ensure_schema(ops: SQLiteResource) -> None:
    """Create ``scrape_targets`` if absent (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_targets (
                username      TEXT PRIMARY KEY,
                profile_url   TEXT NOT NULL,
                results_type  TEXT NOT NULL DEFAULT 'details',
                results_limit INTEGER NOT NULL DEFAULT 1,
                enabled       INTEGER NOT NULL DEFAULT 1,
                tier          TEXT NOT NULL DEFAULT 'tier1',
                updated_at    TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def list_targets(ops: SQLiteResource) -> list[dict]:
    """Return every scrape target, most-recently-updated first."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT username, profile_url, results_type, results_limit, "
            "enabled, tier, updated_at "
            "FROM scrape_targets ORDER BY updated_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def upsert_target(
    ops: SQLiteResource,
    *,
    username: str,
    profile_url: str,
    results_type: str = "details",
    results_limit: int = 1,
    enabled: bool = True,
    tier: str = "tier1",
) -> None:
    """Insert or replace a scrape target."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO scrape_targets
                (username, profile_url, results_type, results_limit,
                 enabled, tier, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                username,
                profile_url,
                results_type,
                results_limit,
                int(enabled),
                tier,
                datetime.now(timezone.utc).isoformat(),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def delete_target(ops: SQLiteResource, username: str) -> None:
    """Remove a scrape target by username (no-op if absent)."""
    conn = ops.get_connection()
    try:
        conn.execute("DELETE FROM scrape_targets WHERE username = ?", [username])
        conn.commit()
    finally:
        conn.close()


def enabled_targets(ops: SQLiteResource) -> list[dict]:
    """Return enabled targets for datalake ingestion."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT username, profile_url, results_type, results_limit, tier "
            "FROM scrape_targets WHERE enabled = 1 ORDER BY username"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def scrape_details_to_bronze(
    profile_url: str,
    *,
    token: str,
    results_limit: int = 1,
) -> str:
    """Run a details-type Apify scrape for one profile → bronze Parquet.

    Returns the dataset_id. Idempotent: skips if the Parquet already exists.
    """
    import json as _json
    from datetime import datetime

    import polars as pl

    from ..common.apify import poll_run, stream_dataset, trigger_run
    from ..common.lake import BRONZE_LAKE, bronze_path

    run = trigger_run(
        "apify~instagram-scraper",
        [profile_url],
        token=token,
        results_limit=results_limit,
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
            "urls": [profile_url],
            "results_limit": results_limit,
            "results_type": "details",
        },
        "downloaded_at": datetime.now().astimezone().isoformat(),
    }
    dest.with_suffix(".parquet.meta").write_text(_json.dumps(meta, indent=2))
    return dataset_id
