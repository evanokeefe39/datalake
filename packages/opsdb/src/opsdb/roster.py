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
Column schema mirrors ``schema.SQLITE_TABLES``.

Named ``roster`` rather than ``creators`` because "creators" is the table and
"roster" is the concern: this module answers "who do we track, at what depth,
and on which platforms" — a question that outlives any one platform's scraper.
Per-platform scraping (the Apify runner) lives in the orchestration layer, not
here: this module touches only the database.
"""

from __future__ import annotations

from datetime import UTC, datetime

from .schema import ConnectionFactory, sqlite_ddl

# Depth sentinel for ad-hoc (non-continuous) local ingestion: the profile's
# posts were ingested once from disk; never schedule a continuous scrape.
AD_HOC_LIMIT = -1


def is_ad_hoc(results_limit: int) -> bool:
    """True when ``results_limit`` marks an ad-hoc (non-continuous) profile."""
    return results_limit == AD_HOC_LIMIT


# Default depth for a newly added profile (one post's worth of detail).
DEFAULT_DEPTH = 1


def _now_iso() -> str:
    """Current UTC timestamp as ISO 8601 string."""
    return datetime.now(UTC).isoformat()


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


def ensure_schema(ops: ConnectionFactory) -> None:
    """Create ``creators`` and ``profiles`` if absent (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("creators"))
        conn.execute(sqlite_ddl("profiles"))
        conn.commit()
    finally:
        conn.close()


# ── Creators ────────────────────────────────────────────────────────────────


def create_creator(ops: ConnectionFactory, name: str) -> dict:
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


def list_creators(ops: ConnectionFactory) -> list[dict]:
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


def get_creator(ops: ConnectionFactory, creator_id: int) -> dict | None:
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


def rename_creator(ops: ConnectionFactory, creator_id: int, name: str) -> dict | None:
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


def remove_creator(ops: ConnectionFactory, creator_id: int) -> None:
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
    ops: ConnectionFactory,
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
    if results_limit != AD_HOC_LIMIT and results_limit < 1:
        raise ValueError("depth must be ≥ 1, or -1 (AD_HOC_LIMIT) for ad-hoc ingestion")
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
    ops: ConnectionFactory,
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
    ops: ConnectionFactory, *, platform: str, handle: str, results_limit: int
) -> dict | None:
    """Change a profile's depth. Returns the updated row, or ``None`` if absent."""
    if results_limit != AD_HOC_LIMIT and results_limit < 1:
        raise ValueError("depth must be ≥ 1, or -1 (AD_HOC_LIMIT) for ad-hoc ingestion")
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


def creator_map(ops: ConnectionFactory, platform: str = "instagram") -> dict[str, dict]:
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


def remove_profile(ops: ConnectionFactory, *, platform: str, handle: str) -> None:
    """Remove a profile (no-op if absent). Does not remove its creator."""
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        conn.execute("DELETE FROM profiles WHERE platform = ? AND handle = ?", [platform, handle])
        conn.commit()
    finally:
        conn.close()


def enabled_profiles(ops: ConnectionFactory, *, include_ad_hoc: bool = False) -> list[dict]:
    """Return enabled profiles for datalake ingestion.

    Ad-hoc profiles (``results_limit = AD_HOC_LIMIT``) are excluded by
    default: they were already ingested from local disk and must never be
    scheduled for a continuous Apify scrape. Pass ``include_ad_hoc=True``
    when the caller needs the full roster regardless of scrape mode.
    """
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        sql = (
            "SELECT platform, handle, profile_url, results_type, results_limit, tier "
            "FROM profiles WHERE enabled = 1"
        )
        params: list = []
        if not include_ad_hoc:
            sql += " AND results_limit != ?"
            params.append(AD_HOC_LIMIT)
        sql += " ORDER BY handle"
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def all_profiles(ops: ConnectionFactory) -> list[dict]:
    """Every profile with its creator columns, including disabled and ad-hoc.

    The ROSTER as a datum: unlike :func:`enabled_profiles` (which answers "what
    should the pipeline scrape?"), this is the whole registry the owner
    maintains. It is what the dashboard serves over ``GET /api/roster`` and what
    the pipeline lands as a bronze source, so both sides read the same list.
    """
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            """
            SELECT c.id AS creator_id,
                   c.name AS creator_name,
                   p.platform,
                   p.handle,
                   p.profile_url,
                   p.results_type,
                   p.results_limit,
                   p.enabled,
                   p.tier,
                   p.updated_at
            FROM profiles p
            JOIN creators c ON c.id = p.creator_id
            ORDER BY p.handle
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def creator_for_handle(ops: ConnectionFactory, *, platform: str, handle: str) -> dict | None:
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
