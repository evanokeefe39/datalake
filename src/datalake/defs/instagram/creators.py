"""Creators + profiles control tables — the multi-platform tracked roster.

Replaces the single ``scrape_targets`` table with two linked tables in
``ops.sqlite``:

* ``creators`` — a person/brand (``id``, human-facing ``name``).
* ``profiles``  — one account on one platform, linked to a creator.
  Identity = ``(platform, handle)``. Carries the scrape config
  (``results_type``, ``results_limit`` = depth, ``enabled``, ``tier``).

A creator owns 1..N profiles across platforms. Today every profile is an
Instagram account, so creator↔profile is 1:1; the split absorbs future
TikTok/YouTube accounts without rework.

The dashboard CRUD endpoints and ``ig_profiles_slv`` read/write these tables.
Column schema mirrors ``common.schemas.SQLITE_TABLES``.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..common.resources import SQLiteResource
from ..common.schemas import sqlite_ddl

# Default depth for a newly added profile (one post's worth of detail).
DEFAULT_DEPTH = 1


def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _default_profile_url(platform: str, handle: str) -> str:
    """Derive a profile URL for a platform+handle when none is supplied.

    Instagram is the only platform scraped today; the others are placeholders
    rendered as "not scraped" until multi-source ingestion lands.
    """
    if platform == "instagram":
        return f"https://www.instagram.com/{handle}/"
    if platform == "tiktok":
        return f"https://www.tiktok.com/@{handle}"
    if platform == "youtube":
        return f"https://www.youtube.com/@{handle}"
    return f"https://www.{platform}.com/{handle}"


def ensure_schema(ops: SQLiteResource) -> None:
    """Create ``creators`` and ``profiles`` if absent (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("creators"))
        conn.execute(sqlite_ddl("profiles"))
        conn.commit()
    finally:
        conn.close()


# ── Creators ────────────────────────────────────────────────────────────────


def create_creator(ops: SQLiteResource, name: str) -> dict:
    """Insert a creator, or return the existing one when ``name`` matches.

    Never duplicates: a pre-existing name is an upsert — the row is kept and
    ``updated_at`` refreshed.
    """
    ensure_schema(ops)
    name = name.strip()
    if not name:
        raise ValueError("creator name must not be empty")
    conn = ops.get_connection()
    try:
        existing = conn.execute(
            "SELECT id, name, created_at, updated_at FROM creators WHERE name = ?",
            [name],
        ).fetchone()
        if existing is not None:
            now = _now_iso()
            conn.execute("UPDATE creators SET updated_at = ? WHERE id = ?", [now, existing["id"]])
            conn.commit()
            row = dict(existing)
            row["updated_at"] = now
            return row
        now = _now_iso()
        cur = conn.execute(
            "INSERT INTO creators (name, created_at, updated_at) VALUES (?, ?, ?)",
            [name, now, now],
        )
        conn.commit()
        return {"id": cur.lastrowid, "name": name, "created_at": now, "updated_at": now}
    finally:
        conn.close()


def list_creators(ops: SQLiteResource) -> list[dict]:
    """Return every creator with its profile count, most-recently-updated first."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.name, c.created_at, c.updated_at,
                   COUNT(p.handle) AS profile_count
            FROM creators c
            LEFT JOIN profiles p ON p.creator_id = c.id
            GROUP BY c.id, c.name, c.created_at, c.updated_at
            ORDER BY c.name COLLATE NOCASE
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_creator(ops: SQLiteResource, creator_id: int) -> dict | None:
    """Return one creator with its profiles, or ``None`` if absent."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT id, name, created_at, updated_at FROM creators WHERE id = ?",
            [creator_id],
        ).fetchone()
        if row is None:
            return None
        creator = dict(row)
        profiles = conn.execute(
            """
            SELECT platform, handle, profile_url, results_type, results_limit,
                   enabled, tier, creator_id, updated_at
            FROM profiles WHERE creator_id = ? ORDER BY platform, handle
            """,
            [creator_id],
        ).fetchall()
        creator["profiles"] = [dict(p) for p in profiles]
        return creator
    finally:
        conn.close()


def rename_creator(ops: SQLiteResource, creator_id: int, name: str) -> dict | None:
    """Rename a creator. Profile membership and scrape config are untouched."""
    ensure_schema(ops)
    name = name.strip()
    if not name:
        raise ValueError("creator name must not be empty")
    conn = ops.get_connection()
    try:
        now = _now_iso()
        cur = conn.execute(
            "UPDATE creators SET name = ?, updated_at = ? WHERE id = ?",
            [name, now, creator_id],
        )
        conn.commit()
        if cur.rowcount == 0:
            return None
        return {"id": creator_id, "name": name, "updated_at": now}
    finally:
        conn.close()


def remove_creator(ops: SQLiteResource, creator_id: int) -> None:
    """Remove a creator and its profiles (no-op if absent)."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        conn.execute("DELETE FROM creators WHERE id = ?", [creator_id])
        conn.commit()
    finally:
        conn.close()


# ── Profiles ────────────────────────────────────────────────────────────────


def add_profile(
    ops: SQLiteResource,
    *,
    creator_id: int,
    platform: str,
    handle: str,
    results_type: str = "details",
    results_limit: int = DEFAULT_DEPTH,
    enabled: bool = True,
    tier: str = "tier1",
    profile_url: str | None = None,
) -> dict:
    """Insert or replace a profile for a creator (upsert on platform+handle)."""
    if results_limit < 1:
        raise ValueError("depth must be ≥ 1")
    ensure_schema(ops)
    handle = handle.strip().lstrip("@")
    if not handle:
        raise ValueError("profile handle must not be empty")
    profile_url = profile_url or _default_profile_url(platform, handle)
    conn = ops.get_connection()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO profiles
                (platform, handle, profile_url, results_type, results_limit,
                 enabled, tier, creator_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                platform,
                handle,
                profile_url,
                results_type,
                results_limit,
                int(enabled),
                tier,
                creator_id,
                _now_iso(),
            ],
        )
        conn.commit()
        return {
            "platform": platform,
            "handle": handle,
            "profile_url": profile_url,
            "results_type": results_type,
            "results_limit": results_limit,
            "enabled": int(enabled),
            "tier": tier,
            "creator_id": creator_id,
        }
    finally:
        conn.close()


def batch_add_profiles(
    ops: SQLiteResource,
    *,
    creator_id: int,
    platform: str,
    handles: list[str],
    results_type: str = "details",
    results_limit: int = DEFAULT_DEPTH,
    enabled: bool = True,
    tier: str = "tier1",
) -> list[dict]:
    """Attach many handles to a creator at once (upsert, same defaults)."""
    return [
        add_profile(
            ops,
            creator_id=creator_id,
            platform=platform,
            handle=h,
            results_type=results_type,
            results_limit=results_limit,
            enabled=enabled,
            tier=tier,
        )
        for h in handles
        if (h or "").strip()
    ]


def edit_depth(
    ops: SQLiteResource, *, platform: str, handle: str, results_limit: int
) -> dict | None:
    """Change a profile's depth. Returns the updated row, or ``None`` if absent."""
    if results_limit < 1:
        raise ValueError("depth must be ≥ 1")
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        cur = conn.execute(
            "UPDATE profiles SET results_limit = ?, updated_at = ? "
            "WHERE platform = ? AND handle = ?",
            [results_limit, _now_iso(), platform, handle],
        )
        conn.commit()
        if cur.rowcount == 0:
            return None
        row = conn.execute(
            "SELECT platform, handle, profile_url, results_type, results_limit, "
            "enabled, tier, creator_id, updated_at "
            "FROM profiles WHERE platform = ? AND handle = ?",
            [platform, handle],
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def creator_map(ops: SQLiteResource, platform: str = "instagram") -> dict[str, dict]:
    """Return ``{handle: {creator_id, creator_name}}`` for a platform's profiles."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            """
            SELECT p.handle, c.id AS creator_id, c.name AS creator_name
            FROM profiles p
            JOIN creators c ON c.id = p.creator_id
            WHERE p.platform = ?
            """,
            [platform],
        ).fetchall()
        return {
            r["handle"]: {"creator_id": r["creator_id"], "creator_name": r["creator_name"]}
            for r in rows
        }
    finally:
        conn.close()


def remove_profile(ops: SQLiteResource, *, platform: str, handle: str) -> None:
    """Remove a profile (no-op if absent). Does not remove its creator."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        conn.execute("DELETE FROM profiles WHERE platform = ? AND handle = ?", [platform, handle])
        conn.commit()
    finally:
        conn.close()


def enabled_profiles(ops: SQLiteResource) -> list[dict]:
    """Return enabled profiles for datalake ingestion."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT platform, handle, profile_url, results_type, results_limit, tier "
            "FROM profiles WHERE enabled = 1 ORDER BY handle"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def creator_for_handle(ops: SQLiteResource, *, platform: str, handle: str) -> dict | None:
    """Return ``{creator_id, creator_name}`` for a platform+handle, or ``None``."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        row = conn.execute(
            """
            SELECT c.id AS creator_id, c.name AS creator_name
            FROM profiles p
            JOIN creators c ON c.id = p.creator_id
            WHERE p.platform = ? AND p.handle = ?
            """,
            [platform, handle],
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


# ── Instagram-specific scrape runner ─────────────────────────────────────────


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
