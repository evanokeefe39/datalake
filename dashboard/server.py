"""
FastAPI data server for the Lakehouse dashboard.
Connects to DuckDB and exposes analytics views as JSON endpoints.
Run with: uv run uvicorn server:app --port 3002 --reload
"""

from __future__ import annotations

import logging
import os
import sqlite3
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from datalake.defs.common.lake import (
    AVATAR_DIR,
    THUMBNAIL_DIR,
    avatar_path,
    thumbnail_path,
)
from datalake.defs.common.resources import SQLiteResource
from datalake.defs.instagram.creators import (
    add_profile,
    batch_add_profiles,
    create_creator,
    edit_depth,
    get_creator,
    list_creators,
    remove_creator,
    remove_profile,
    rename_creator,
    scrape_details_to_bronze,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard-api")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "state.duckdb"
OPS_PATH = Path(__file__).resolve().parent.parent / "data" / "ops.sqlite"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Create media dirs + cache table at startup, not import time."""
    AVATAR_DIR.mkdir(parents=True, exist_ok=True)
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    _ensure_media_cache_table()
    yield


app = FastAPI(title="Lakehouse Dashboard API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001", "http://localhost:3002"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _connect() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DB_PATH}")
    return duckdb.connect(str(DB_PATH), read_only=True)


def _ops_connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(OPS_PATH))
    con.row_factory = sqlite3.Row
    return con


# ── Media Cache ─────────────────────────────────────────────────
#
# Instagram CDN URLs expire in ~4-5 days, so the dashboard caches image
# *bytes* to disk — never the URLs. Two endpoints:
#   - thumbnails: fetched from Instagram's public /media/ endpoint on first
#     request, then served from disk (byte-cache, tracked in ops.sqlite).
#   - avatars: populated at pipeline time by ig_profiles_slv; served from
#     disk, or a DiceBear identicon redirect when absent.


def _ensure_media_cache_table() -> None:
    """Idempotent schema creation for the dashboard media cache."""
    con = _ops_connect()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS media_cache (
                cache_key    TEXT PRIMARY KEY,
                local_path   TEXT NOT NULL,
                content_type TEXT,
                size_bytes   INTEGER,
                fetched_at   TEXT NOT NULL,
                source_url   TEXT
            )
        """)
        con.commit()
    finally:
        con.close()


def _cache_media_row(
    cache_key: str,
    local_path: Path,
    content_type: str,
    source_url: str,
) -> None:
    """Record a cached media file in ops.sqlite."""
    con = _ops_connect()
    try:
        con.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                cache_key,
                str(local_path),
                content_type,
                local_path.stat().st_size,
                datetime.now(timezone.utc).isoformat(),
                source_url,
            ],
        )
        con.commit()
    finally:
        con.close()


def _fetch_thumbnail_bytes(shortcode: str) -> tuple[bytes, str] | None:
    """Fetch raw image bytes + content type for a post thumbnail.

    Returns None on non-200, non-image content type, or empty body — the
    caller turns that into a 404.
    """
    url = f"https://www.instagram.com/p/{shortcode}/media/?size=m"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.instagram.com/",
        },
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        logger.warning("Thumbnail fetch failed for %s: %s", shortcode, exc)
        return None
    content_type = resp.headers.get("Content-Type", "")
    if not content_type.startswith("image/"):
        logger.warning("Thumbnail %s returned non-image type %s", shortcode, content_type)
        return None
    body = resp.read()
    if not body:
        logger.warning("Thumbnail %s returned empty body", shortcode)
        return None
    return body, content_type


def _atomic_write(path: Path, data: bytes) -> None:
    """Write bytes atomically (temp file + rename) to avoid partial reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


@app.get("/api/media/avatar/{username}")
def avatar(username: str):
    """Serve a profile picture from disk, or redirect to a DiceBear identicon.

    Avatars are populated at pipeline time (ig_profiles_slv), never fetched
    from Instagram here — CDN URLs expire and can't be refreshed at runtime.
    """
    local = avatar_path(username)
    if local.exists() and local.stat().st_size > 0:
        return FileResponse(local, media_type="image/jpeg")
    fallback = (
        f"https://api.dicebear.com/9.x/identicon/svg"
        f"?seed={username}&backgroundColor=000000&foregroundColor=00ffff"
    )
    return RedirectResponse(url=fallback, status_code=302)


@app.get("/api/media/thumbnail/{shortcode}")
def thumbnail(shortcode: str):
    """Serve a post thumbnail, byte-caching from Instagram on first request."""
    local = thumbnail_path(shortcode)
    if local.exists() and local.stat().st_size > 0:
        return FileResponse(local, media_type="image/jpeg")

    fetched = _fetch_thumbnail_bytes(shortcode)
    if fetched is None:
        raise HTTPException(status_code=404, detail="thumbnail unavailable")
    body, content_type = fetched

    _atomic_write(local, body)
    _cache_media_row(
        f"thumb:{shortcode}",
        local,
        content_type,
        f"https://www.instagram.com/p/{shortcode}/media/?size=m",
    )
    return FileResponse(local, media_type=content_type)


# ── Health ──────────────────────────────────────────────────────


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── Overview Metrics ───────────────────────────────────────────


@app.get("/api/overview")
def overview():
    db = _connect()
    try:
        total_posts = db.execute("SELECT COUNT(*) FROM silver_ig_posts").fetchone()[0]

        total_enriched = db.execute(
            "SELECT COUNT(*) FROM gold_analyses WHERE domain = 'instagram'"
        ).fetchone()[0]

        total_profiles = db.execute(
            "SELECT COUNT(DISTINCT owner_username) FROM silver_ig_posts"
        ).fetchone()[0]

        enrichment_pct = round((total_enriched / total_posts * 100), 1) if total_posts else 0

        admiralty = (
            db.execute(
                "SELECT ROUND(AVG(admiralty_score), 2) FROM v_profile_quality "
                "WHERE enriched_posts > 0"
            ).fetchone()[0]
            or 0
        )

        high_signal = db.execute("SELECT COUNT(*) FROM v_signal").fetchone()[0]

        return {
            "total_posts": total_posts,
            "total_enriched": total_enriched,
            "total_profiles": total_profiles,
            "enrichment_pct": enrichment_pct,
            "avg_admiralty_score": float(admiralty),
            "high_signal_count": high_signal,
        }
    finally:
        db.close()


# ── Profiles ────────────────────────────────────────────────────


@app.get("/api/profiles")
def profiles():
    db = _connect()
    try:
        rows = db.execute("""
            SELECT owner_id, owner_username, creator_id, total_posts, enriched_posts,
                   admiralty_score, educational_rate,
                   avg_likes, avg_comments, avg_video_views, max_likes
            FROM v_profile_quality
            WHERE total_posts > 0
            ORDER BY admiralty_score DESC
        """).fetchall()

        return [
            {
                "owner_id": r[0],
                "owner_username": r[1],
                "creator_id": r[2],
                "total_posts": r[3],
                "enriched_posts": r[4],
                "admiralty_score": float(r[5]) if r[5] else 0,
                "educational_rate": float(r[6]) if r[6] else 0,
                "avg_likes": float(r[7]) if r[7] else 0,
                "avg_comments": float(r[8]) if r[8] else 0,
                "avg_video_views": float(r[9]) if r[9] else 0,
                "max_likes": r[10] or 0,
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Signals ─────────────────────────────────────────────────────


@app.get("/api/signals")
def signals():
    db = _connect()
    try:
        rows = db.execute("""
            SELECT post_id, owner_username, creator_id, admiralty, gold_domain, gold_topic,
                   is_educational, is_actionable,
                   caption, likes_count, comments_count, video_view_count,
                   shortcode
            FROM v_signal
            ORDER BY
                CASE
                    WHEN admiralty LIKE 'A%' THEN 1
                    WHEN admiralty LIKE 'B%' THEN 2
                    ELSE 3
                END,
                admiralty
        """).fetchall()

        return [
            {
                "post_id": r[0],
                "owner_username": r[1],
                "creator_id": r[2],
                "admiralty": r[3],
                "gold_domain": r[4],
                "gold_topic": r[5],
                "is_educational": bool(r[6]) if r[6] is not None else False,
                "is_actionable": bool(r[7]) if r[7] is not None else False,
                "caption": r[8] or "",
                "likes_count": r[9] or 0,
                "comments_count": r[10] or 0,
                "video_view_count": r[11] or 0,
                "shortcode": r[12] or "",
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Posts ───────────────────────────────────────────────────────


@app.get("/api/posts")
def posts(
    limit: int = Query(0, ge=0, le=5000),
    offset: int = Query(0, ge=0),
    username: str | None = Query(None),
    sort: str | None = Query(None),
    order: str | None = Query("desc"),
):
    db = _connect()
    try:
        where = ""
        params: list = []
        if username:
            where = "WHERE v.owner_username = ?"
            params.append(username)

        order_clause = "ORDER BY v.timestamp DESC"
        if sort:
            safe_sort = (
                sort
                if sort
                in (
                    "likes_count",
                    "comments_count",
                    "video_view_count",
                    "timestamp",
                    "owner_username",
                    "admiralty",
                )
                else "timestamp"
            )
            safe_order = "DESC" if order and order.upper() == "DESC" else "ASC"
            order_clause = f"ORDER BY v.{safe_sort} {safe_order}"

        limit_clause = ""
        if limit > 0:
            limit_clause = f"LIMIT {int(limit)} OFFSET {int(offset)}"

        rows = db.execute(
            f"""
            SELECT v.post_id, v.owner_username, v.creator_id, v.caption,
                   v.likes_count, v.comments_count, v.video_view_count,
                   v.is_educational, v.is_actionable,
                   v.admiralty, v.gold_domain, v.gold_topic, v.gold_subtopic,
                   v.content_type, v.style, v.format,
                   v.gold_analysed_at, v.timestamp, v.shortcode
            FROM v_post_detail v
            {where}
            {order_clause}
            {limit_clause}
        """,
            params,
        ).fetchall()

        return [
            {
                "post_id": r[0],
                "owner_username": r[1],
                "creator_id": r[2],
                "caption": r[3] or "",
                "likes_count": r[4] or 0,
                "comments_count": r[5] or 0,
                "video_view_count": r[6] or 0,
                "is_educational": bool(r[7]) if r[7] is not None else None,
                "is_actionable": bool(r[8]) if r[8] is not None else None,
                "admiralty": r[9],
                "gold_domain": r[10],
                "gold_topic": r[11],
                "gold_subtopic": r[12],
                "content_type": r[13],
                "style": r[14],
                "format": r[15],
                "analysed_at": r[16],
                "timestamp": str(r[17]) if r[17] else None,
                "shortcode": r[18] or "",
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Full-text Search ────────────────────────────────────────────


@app.get("/api/search")
def search_posts(
    q: str = Query(..., min_length=2),
    limit: int = Query(500, ge=1, le=5000),
):
    """Full-text search across captions, topics, and usernames via DuckDB LIKE."""
    db = _connect()
    try:
        pattern = f"%{q}%"
        rows = db.execute(
            """
            SELECT v.post_id, v.owner_username, v.caption,
                   v.likes_count, v.comments_count, v.video_view_count,
                   v.is_educational, v.is_actionable,
                   v.admiralty, v.gold_domain, v.gold_topic, v.gold_subtopic,
                   v.content_type, v.style, v.format,
                   v.gold_analysed_at, v.timestamp, v.shortcode
            FROM v_post_detail v
            WHERE v.caption ILIKE ?
               OR v.owner_username ILIKE ?
               OR v.gold_topic ILIKE ?
               OR v.gold_domain ILIKE ?
            ORDER BY v.timestamp DESC
            LIMIT ?
        """,
            [pattern, pattern, pattern, pattern, limit],
        ).fetchall()

        return [
            {
                "post_id": r[0],
                "owner_username": r[1],
                "caption": r[2] or "",
                "likes_count": r[3] or 0,
                "comments_count": r[4] or 0,
                "video_view_count": r[5] or 0,
                "is_educational": bool(r[6]) if r[6] is not None else None,
                "is_actionable": bool(r[7]) if r[7] is not None else None,
                "admiralty": r[8],
                "gold_domain": r[9],
                "gold_topic": r[10],
                "gold_subtopic": r[11],
                "content_type": r[12],
                "style": r[13],
                "format": r[14],
                "analysed_at": r[15],
                "timestamp": str(r[16]) if r[16] else None,
                "shortcode": r[17] or "",
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Standout Posts ( >1σ above creator mean ) ──────────────────


@app.get("/api/standout-posts")
def standout_posts(limit: int = Query(20, ge=1, le=100)):
    """Posts exceeding 1 standard deviation above their creator's mean likes."""
    db = _connect()
    try:
        rows = db.execute(
            """
            WITH creator_stats AS (
                SELECT owner_username,
                       AVG(likes_count) AS mean_likes,
                       STDDEV(likes_count) AS std_likes
                FROM silver_ig_posts
                WHERE likes_count > 0
                GROUP BY owner_username
                HAVING COUNT(*) >= 3
            ),
            standouts AS (
                SELECT sp.post_id, sp.owner_username, sp.shortcode,
                       sp.caption, sp.likes_count, sp.comments_count,
                       sp.video_view_count, sp.timestamp,
                       cs.mean_likes, cs.std_likes,
                       ROUND(
                           (sp.likes_count - cs.mean_likes)
                           / NULLIF(cs.std_likes, 0),
                           2
                       ) AS z_score
                FROM silver_ig_posts sp
                JOIN creator_stats cs ON sp.owner_username = cs.owner_username
                WHERE sp.likes_count > cs.mean_likes + cs.std_likes
            )
            SELECT * FROM standouts
            ORDER BY z_score DESC
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [
            {
                "post_id": r[0],
                "owner_username": r[1],
                "shortcode": r[2] or "",
                "caption": (r[3] or "")[:120],
                "likes_count": r[4] or 0,
                "comments_count": r[5] or 0,
                "video_view_count": r[6] or 0,
                "timestamp": str(r[7]) if r[7] else None,
                "mean_likes": round(r[8], 0) if r[8] else 0,
                "std_likes": round(r[9], 0) if r[9] else 0,
                "z_score": float(r[10]) if r[10] else 0,
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Weekly Summary (standout posts per day of month) ────────────


@app.get("/api/weekly-summary")
def weekly_summary():
    """Standout posts grouped by day of month for the current month."""
    db = _connect()
    try:
        rows = db.execute("""
            WITH creator_stats AS (
                SELECT owner_username,
                       AVG(likes_count) AS mean_likes,
                       STDDEV(likes_count) AS std_likes
                FROM silver_ig_posts
                WHERE likes_count > 0
                GROUP BY owner_username
                HAVING COUNT(*) >= 3
            ),
            standouts AS (
                SELECT sp.post_id, sp.owner_username,
                       sp.likes_count,
                       EXTRACT(DAY FROM sp.timestamp) AS day_of_month,
                       sp.timestamp
                FROM silver_ig_posts sp
                JOIN creator_stats cs ON sp.owner_username = cs.owner_username
                WHERE sp.likes_count > cs.mean_likes + cs.std_likes
            )
            SELECT day_of_month, COUNT(*) AS standout_count
            FROM standouts
            GROUP BY day_of_month
            ORDER BY day_of_month
        """).fetchall()

        return [{"day": int(r[0]), "standout_count": r[1]} for r in rows]
    finally:
        db.close()


# ── Recent Standouts by Creator ─────────────────────────────────


@app.get("/api/recent-standouts")
def recent_standouts(limit: int = Query(10, ge=1, le=50)):
    """Recent standout posts grouped by creator, for homepage cards."""
    db = _connect()
    try:
        rows = db.execute(
            """
            WITH creator_stats AS (
                SELECT owner_username,
                       AVG(likes_count) AS mean_likes,
                       STDDEV(likes_count) AS std_likes
                FROM silver_ig_posts
                WHERE likes_count > 0
                GROUP BY owner_username
                HAVING COUNT(*) >= 3
            ),
            standouts AS (
                SELECT sp.post_id, sp.owner_username, sp.shortcode,
                       sp.caption, sp.likes_count, sp.comments_count,
                       sp.timestamp,
                       cs.mean_likes, cs.std_likes,
                       ROUND(
                           (sp.likes_count - cs.mean_likes)
                           / NULLIF(cs.std_likes, 0),
                           2
                       ) AS z_score,
                       ROW_NUMBER() OVER (
                           PARTITION BY sp.owner_username
                           ORDER BY (sp.likes_count - cs.mean_likes) / NULLIF(cs.std_likes, 0) DESC
                       ) AS rn
                FROM silver_ig_posts sp
                JOIN creator_stats cs ON sp.owner_username = cs.owner_username
                WHERE sp.likes_count > cs.mean_likes + cs.std_likes
            )
            SELECT post_id, owner_username, shortcode, caption,
                   likes_count, comments_count, timestamp,
                   mean_likes, std_likes, z_score
            FROM standouts
            WHERE rn <= 3
            ORDER BY z_score DESC
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [
            {
                "post_id": r[0],
                "owner_username": r[1],
                "shortcode": r[2] or "",
                "caption": (r[3] or "")[:120],
                "likes_count": r[4] or 0,
                "comments_count": r[5] or 0,
                "timestamp": str(r[6]) if r[6] else None,
                "mean_likes": round(r[7], 0) if r[7] else 0,
                "std_likes": round(r[8], 0) if r[8] else 0,
                "z_score": float(r[9]) if r[9] else 0,
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Creators + Profiles (profile management) ────────────────────


class CreatorIn(BaseModel):
    """Request body for creating or renaming a creator."""

    name: str


class ProfileIn(BaseModel):
    """Request body for adding a profile to a creator."""

    platform: str = "instagram"
    handle: str
    results_type: str = "details"
    results_limit: int = Field(1, ge=1)
    enabled: bool = True
    tier: str = "tier1"


class BatchProfilesIn(BaseModel):
    """Request body for batch-adding profiles to a creator."""

    platform: str = "instagram"
    handles: list[str]
    results_type: str = "details"
    results_limit: int = Field(1, ge=1)
    enabled: bool = True
    tier: str = "tier1"


class DepthIn(BaseModel):
    """Request body for editing a profile's depth."""

    results_limit: int = Field(1, ge=1)


def _ops_resource() -> SQLiteResource:
    """SQLiteResource bound to the dashboard's ops database."""
    return SQLiteResource(database=str(OPS_PATH))


def _run_details_scrape(profile_url: str) -> None:
    """Background: details scrape → bronze (never raises into the request)."""
    token = os.environ.get("APIFY_API_TOKEN", "")
    if not token:
        logger.warning("Skipping details scrape for %s — no APIFY_API_TOKEN", profile_url)
        return
    try:
        dataset_id = scrape_details_to_bronze(profile_url, token=token)
        logger.info("Details scrape for %s → dataset %s", profile_url, dataset_id)
    except Exception as exc:  # noqa: BLE001 — background task must not crash
        logger.error("Details scrape failed for %s: %s", profile_url, exc)


def _extract_handle(value: str) -> str:
    """Normalize a profile handle, accepting a bare handle or a full URL."""
    h = value.strip().lstrip("@")
    if h.startswith("http"):
        h = h.rstrip("/").split("/")[-1].split("?")[0]
    return h


def _post_counts(db: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Map owner_username → post count from silver."""
    rows = db.execute(
        "SELECT owner_username, COUNT(*) FROM silver_ig_posts "
        "WHERE owner_username IS NOT NULL GROUP BY owner_username"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _profiles_by_creator(ops: SQLiteResource) -> dict[int, list[dict]]:
    """Map creator_id → list of {platform, handle}."""
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT creator_id, platform, handle FROM profiles ORDER BY handle"
        ).fetchall()
    finally:
        conn.close()
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(r["creator_id"], []).append(
            {"platform": r["platform"], "handle": r["handle"]}
        )
    return out


@app.get("/api/creators")
def creators():
    db = _connect()
    try:
        rows = list_creators(_ops_resource())
        grouped = _profiles_by_creator(_ops_resource())
        post_counts = _post_counts(db)
        result = []
        for c in rows:
            profiles = grouped.get(c["id"], [])
            platforms = sorted({p["platform"] for p in profiles})
            total_posts = sum(
                post_counts.get(p["handle"], 0)
                for p in profiles
                if p["platform"] == "instagram"
            )
            result.append(
                {
                    "id": c["id"],
                    "name": c["name"],
                    "created_at": c["created_at"],
                    "updated_at": c["updated_at"],
                    "profile_count": c["profile_count"],
                    "platforms": platforms,
                    "total_posts": total_posts,
                }
            )
        return result
    finally:
        db.close()


@app.get("/api/creators/{creator_id}")
def creator_detail(creator_id: int):
    creator = get_creator(_ops_resource(), creator_id)
    if creator is None:
        raise HTTPException(status_code=404, detail="Creator not found")

    db = _connect()
    try:
        post_counts = _post_counts(db)
        profiles_out = []
        for p in creator["profiles"]:
            profile = dict(p)
            profile["post_count"] = (
                post_counts.get(p["handle"], 0) if p["platform"] == "instagram" else 0
            )
            if p["platform"] == "instagram":
                meta = db.execute(
                    "SELECT full_name, biography, followers_count, posts_count "
                    "FROM silver_ig_profiles WHERE owner_username = ?",
                    [p["handle"]],
                ).fetchone()
                if meta:
                    profile["full_name"] = meta[0]
                    profile["biography"] = meta[1]
                    profile["followers_count"] = meta[2]
                    profile["posts_count"] = meta[3]
            profiles_out.append(profile)
        creator["profiles"] = profiles_out
        return creator
    finally:
        db.close()


@app.post("/api/creators", status_code=201)
def add_creator(payload: CreatorIn):
    return create_creator(_ops_resource(), payload.name)


@app.patch("/api/creators/{creator_id}")
def update_creator(creator_id: int, payload: CreatorIn):
    creator = rename_creator(_ops_resource(), creator_id, payload.name)
    if creator is None:
        raise HTTPException(status_code=404, detail="Creator not found")
    return creator


@app.delete("/api/creators/{creator_id}")
def delete_creator(creator_id: int):
    remove_creator(_ops_resource(), creator_id)
    return {"id": creator_id, "status": "deleted"}


@app.post("/api/creators/{creator_id}/profiles", status_code=201)
def add_creator_profile(
    creator_id: int, payload: ProfileIn, background: BackgroundTasks
):
    if get_creator(_ops_resource(), creator_id) is None:
        raise HTTPException(status_code=404, detail="Creator not found")
    profile = add_profile(
        _ops_resource(),
        creator_id=creator_id,
        platform=payload.platform,
        handle=_extract_handle(payload.handle),
        results_type=payload.results_type,
        results_limit=payload.results_limit,
        enabled=payload.enabled,
        tier=payload.tier,
    )
    if payload.enabled and payload.platform == "instagram":
        background.add_task(_run_details_scrape, profile["profile_url"])
    return profile


@app.post("/api/creators/{creator_id}/profiles/batch", status_code=201)
def add_creator_profiles_batch(creator_id: int, payload: BatchProfilesIn):
    if get_creator(_ops_resource(), creator_id) is None:
        raise HTTPException(status_code=404, detail="Creator not found")
    profiles = batch_add_profiles(
        _ops_resource(),
        creator_id=creator_id,
        platform=payload.platform,
        handles=[_extract_handle(h) for h in payload.handles],
        results_type=payload.results_type,
        results_limit=payload.results_limit,
        enabled=payload.enabled,
        tier=payload.tier,
    )
    return {"creator_id": creator_id, "added": len(profiles), "profiles": profiles}


@app.patch("/api/profiles/{platform}/{handle}")
def update_profile_depth(platform: str, handle: str, payload: DepthIn):
    profile = edit_depth(
        _ops_resource(),
        platform=platform,
        handle=handle,
        results_limit=payload.results_limit,
    )
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found")
    return profile


@app.delete("/api/profiles/{platform}/{handle}")
def delete_profile(platform: str, handle: str):
    remove_profile(_ops_resource(), platform=platform, handle=handle)
    return {"platform": platform, "handle": handle, "status": "deleted"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3002)
