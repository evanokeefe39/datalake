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
from datalake.defs.common.schemas import sqlite_ddl
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
        con.execute(sqlite_ddl("media_cache"))
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
    """Thin projector over the canonical single-row ``v_overview`` view."""
    db = _connect()
    try:
        r = db.execute("SELECT * FROM v_overview").fetchone()
        return {
            "total_posts": r[0],
            "total_enriched": r[1],
            "total_profiles": r[2],
            "enrichment_pct": r[3] if r[3] is not None else 0,
            "avg_admiralty_score": float(r[4] or 0),
            "high_signal_count": r[5],
        }
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
                   shortcode, channel
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
                "platform": r[13] or "instagram",
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Posts ───────────────────────────────────────────────────────


_POST_SELECT = """
    SELECT v.post_id, v.owner_username, v.creator_id, v.caption,
           v.likes_count, v.comments_count, v.video_view_count,
           v.is_educational, v.is_actionable,
           v.admiralty, v.gold_domain, v.gold_topic, v.gold_subtopic,
           v.content_type, v.style, v.format,
           v.gold_analysed_at, v.timestamp, v.shortcode, v.channel,
           pm.relative_performance, pm.baseline_q3 AS baseline_likes,
           pm.likes_zscore
    FROM v_post_detail v
    LEFT JOIN v_post_metrics pm ON pm.post_id = v.post_id
"""


def _rows_to_posts(rows) -> list[dict]:
    """Shape ``v_post_detail`` rows into the dashboard PostRow JSON shape."""
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
            "platform": r[19] or "instagram",
            "relative_performance": r[20],
            "baseline_likes": round(r[21], 0) if r[21] is not None else None,
        }
        for r in rows
    ]


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
            f"{_POST_SELECT} {where} {order_clause} {limit_clause}",
            params,
        ).fetchall()

        return _rows_to_posts(rows)
    finally:
        db.close()




@app.get("/api/posts/{post_id}")
def post_detail(post_id: str):
    """Full post context for the detail page — thin projector over the
    canonical views (v_post_detail + v_post_metrics).

    Point-in-time engagement context only: likes_zscore is judged against the
    post's own trailing label-pass baseline (baseline_q3/baseline_iqr), never a
    creator all-time average. Transcript data is not yet available in the
    warehouse (gold result_json / silver meta_data carry no transcript), so
    ``transcript`` is always null for now.
    """
    db = _connect()
    try:
        row = db.execute(
            """
            SELECT v.post_id, v.shortcode, v.url, v.owner_username,
                   v.creator_id, v.creator_name, v.caption, v.timestamp,
                   v.likes_count, v.comments_count, v.video_view_count,
                   v.media_count, v.hashtags,
                   v.admiralty, v.gold_domain, v.gold_subdomain, v.gold_topic,
                   v.gold_subtopic, v.content_type, v.style, v.format,
                   v.is_educational, v.is_actionable, v.gold_analysed_at,
                   v.channel,
                   pm.label, pm.is_provisional, pm.likes_zscore,
                   pm.baseline_q3, pm.baseline_iqr, pm.breakout_multiple,
                   pm.sigma_tier, pm.is_standout, pm.is_hot,
                   pm.relative_performance, pm.owner_rank, pm.is_top3_in_owner
            FROM v_post_detail v
            LEFT JOIN v_post_metrics pm ON pm.post_id = v.post_id
            WHERE v.post_id = ?
            """,
            [post_id],
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="post not found")
        return {
            "post_id": row[0],
            "shortcode": row[1] or "",
            "url": row[2],
            "owner_username": row[3],
            "creator_id": row[4],
            "creator_name": row[5],
            "caption": row[6] or "",
            # Transcript not yet available in the warehouse (no transcript
            # columns in gold result_json / silver meta_data).
            "transcript": None,
            "timestamp": str(row[7]) if row[7] else None,
            "likes_count": row[8] or 0,
            "comments_count": row[9] or 0,
            "video_view_count": row[10] or 0,
            "media_count": row[11] or 0,
            "hashtags": row[12] or "",
            "enrichment": {
                "admiralty": row[13],
                "gold_domain": row[14],
                "gold_subdomain": row[15],
                "gold_topic": row[16],
                "gold_subtopic": row[17],
                "content_type": row[18],
                "style": row[19],
                "format": row[20],
                "is_educational": bool(row[21]) if row[21] is not None else None,
                "is_actionable": bool(row[22]) if row[22] is not None else None,
                "analysed_at": row[23],
            },
            "platform": row[24] or "instagram",
            # Point-in-time engagement context — z-score vs the post's OWN
            # trailing Tukey baseline, not a creator average.
            "point_in_time": {
                "label": row[25],
                "is_provisional": bool(row[26]) if row[26] is not None else None,
                "likes_zscore": float(row[27]) if row[27] is not None else None,
                "baseline_q3": round(row[28], 0) if row[28] is not None else None,
                "baseline_iqr": round(row[29], 0) if row[29] is not None else None,
                "breakout_multiple": round(row[30], 1) if row[30] is not None else None,
                "sigma_tier": row[31],
                "is_standout": bool(row[32]) if row[32] is not None else None,
                "is_hot": bool(row[33]) if row[33] is not None else None,
                "relative_performance": row[34],
                "owner_rank": row[35],
                "is_top3_in_owner": bool(row[36]) if row[36] is not None else None,
            },
        }
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


# ── Standout Posts ( ig_post_labels: label='standout' ) ─────────


@app.get("/api/standout-posts")
def standout_posts(limit: int = Query(20, ge=1, le=100)):
    """Posts labeled standout by the Tukey-fence label pass (ig_post_labels).

    Thin projector over ``v_post_metrics`` — point-in-time context only: each
    post is compared to its own trailing baseline, never a creator average.
    """
    db = _connect()
    try:
        rows = db.execute(
            """
            SELECT post_id, owner_username, shortcode, caption,
                   likes_count, comments_count, video_view_count,
                   timestamp, baseline_q3, baseline_iqr, likes_zscore,
                   method, is_provisional, creator_id, channel
            FROM v_post_metrics
            WHERE is_standout = 1
            ORDER BY CASE WHEN method = 'day7_matched' THEN 0 ELSE 1 END,
                     likes_zscore DESC
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
                # Per-post TRAILING Tukey baseline from the label pass — NOT a
                # mean; exposed with honest names. Point-in-time only.
                "baseline_q3": round(r[8], 0) if r[8] else 0,
                "baseline_iqr": round(r[9], 0) if r[9] else 0,
                "z_score": float(r[10]) if r[10] else 0,
                "method": r[11],
                "provisional": bool(r[12]),
                "creator_id": r[13],
                "platform": r[14] or "instagram",
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Weekly Summary (standout posts per day of month) ────────────


@app.get("/api/weekly-summary")
def weekly_summary():
    """Standout posts per day of month — thin projector over
    ``v_standout_calendar``."""
    db = _connect()
    try:
        rows = db.execute(
            "SELECT day_of_month, standout_count FROM v_standout_calendar "
            "ORDER BY day_of_month"
        ).fetchall()
        return [{"day": int(r[0]), "standout_count": r[1]} for r in rows]
    finally:
        db.close()


# ── Hot Posts ( Recent Hot Posts: 2σ+ standouts, last 28 days ) ──


@app.get("/api/hot-posts")
def hot_posts(limit: int = Query(10, ge=1, le=50)):
    """Recent Hot Posts — 2σ+ standouts from the last 28 days, top-3 per owner.

    Thin projector over ``v_recent_hot_posts`` (recency-weighted in the
    warehouse) — point-in-time context only; no creator all-time average.
    """
    db = _connect()
    try:
        rows = db.execute(
            """
            SELECT post_id, owner_username, shortcode, caption,
                   likes_count, comments_count, timestamp,
                   baseline_q3, baseline_iqr, likes_zscore, breakout_multiple,
                   creator_id, channel
            FROM v_recent_hot_posts
            ORDER BY likes_zscore DESC
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
                # Per-post TRAILING Tukey baseline from the label pass — NOT a
                # mean. Point-in-time context; no creator-avg key on posts.
                "baseline_q3": round(r[7], 0) if r[7] else 0,
                "baseline_iqr": round(r[8], 0) if r[8] else 0,
                "z_score": float(r[9]) if r[9] else 0,
                "breakout_multiple": round(r[10], 1) if r[10] else None,
                "creator_id": r[11],
                "platform": r[12] or "instagram",
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
    """Ops registry (identity) joined to canonical views (metrics).

    Activity metrics (counts, avg_likes, max_likes) come gate-free from
    ``v_creator_metrics``; enrichment-quality columns from ``v_creator_quality``.
    The Python joins below are keyed registry merges, not aggregation.
    """
    db = _connect()
    try:
        rows = list_creators(_ops_resource())
        grouped = _profiles_by_creator(_ops_resource())
        profile = {
            r[0]: {
                "total_posts": int(r[1] or 0),
                "standout_count": int(r[2] or 0),
                "hot_count": int(r[3] or 0),
                "avg_likes": float(r[4]) if r[4] is not None else 0,
                "max_likes": int(r[5] or 0),
                "avg_engagement_score": (
                    float(r[6]) if r[6] is not None else None
                ),
                "dominant_domain": r[7],
                "dominant_domain_posts": int(r[8] or 0),
                "momentum_ratio": float(r[9]) if r[9] is not None else None,
                "is_rising": bool(r[10]),
            }
            for r in db.execute(
                "SELECT creator_id, total_posts, standout_count, hot_count, "
                "avg_likes, max_likes, avg_engagement_score, dominant_domain, "
                "dominant_domain_posts, momentum_ratio, is_rising "
                "FROM v_creator_profile"
            ).fetchall()
        }
        quality = {
            r[0]: {
                "enriched_posts": int(r[1]) if r[1] is not None else 0,
                "educational_rate": float(r[2]) if r[2] is not None else 0,
                "actionable_rate": float(r[3]) if r[3] is not None else 0,
                "admiralty_score": float(r[4]) if r[4] is not None else 0,
            }
            for r in db.execute(
                "SELECT creator_id, enriched_posts, educational_rate, "
                "actionable_rate, admiralty_score FROM v_creator_quality"
            ).fetchall()
        }
        result = []
        for c in rows:
            profiles = grouped.get(c["id"], [])
            platforms = sorted({p["platform"] for p in profiles})
            handles = [p["handle"] for p in profiles]
            m = profile.get(c["id"], {})
            q = quality.get(c["id"], {})
            result.append(
                {
                    "id": c["id"],
                    "name": c["name"],
                    "created_at": c["created_at"],
                    "updated_at": c["updated_at"],
                    "profile_count": c["profile_count"],
                    "platforms": platforms,
                    "total_posts": m.get("total_posts", 0),
                    "standout_count": m.get("standout_count", 0),
                    "hot_count": m.get("hot_count", 0),
                    "avatar_handle": handles[0] if handles else None,
                    "enriched_posts": q.get("enriched_posts", 0),
                    "educational_rate": q.get("educational_rate", 0),
                    "actionable_rate": q.get("actionable_rate", 0),
                    "admiralty_score": q.get("admiralty_score", 0),
                    "avg_likes": m.get("avg_likes", 0),
                    "max_likes": m.get("max_likes", 0),
                    "avg_engagement_score": m.get("avg_engagement_score"),
                    "dominant_domain": m.get("dominant_domain"),
                    "dominant_domain_posts": m.get("dominant_domain_posts", 0),
                    "momentum_ratio": m.get("momentum_ratio"),
                    "is_rising": m.get("is_rising", False),
                }
            )
        return result
    finally:
        db.close()


# ── Top / Rising Creators ──────────────────────────────────────


@app.get("/api/top-creators")
def top_creators():
    """Top 10 creators by composite quality score."""
    db = _connect()
    try:
        rows = db.execute(
            "SELECT creator_id, creator_name, total_posts, enriched_posts, "
            "admiralty_score, educational_rate, actionable_rate, avg_likes, "
            "max_likes, composite_score FROM v_creator_quality "
            "ORDER BY composite_score DESC LIMIT 10"
        ).fetchall()
        return [
            {
                "creator_id": int(r[0]),
                "creator_name": str(r[1]),
                "total_posts": int(r[2]),
                "enriched_posts": int(r[3]),
                "admiralty_score": float(r[4]),
                "educational_rate": float(r[5]),
                "actionable_rate": float(r[6]),
                "avg_likes": float(r[7]),
                "max_likes": int(r[8]),
                "composite_score": float(r[9]),
            }
            for r in rows
        ]
    finally:
        db.close()


@app.get("/api/rising-creators")
def rising_creators():
    """Top 10 creators by momentum (recent vs. baseline average likes)."""
    db = _connect()
    try:
        rows = db.execute(
            "SELECT creator_id, creator_name, recent_avg, recent_posts, "
            "baseline_avg, baseline_posts, momentum_ratio FROM v_rising_creators "
            "ORDER BY momentum_ratio DESC, recent_avg DESC, creator_id ASC LIMIT 10"
        ).fetchall()
        result = [
            {
                "creator_id": int(r[0]),
                "creator_name": str(r[1]),
                "recent_avg": float(r[2]),
                "recent_posts": int(r[3]),
                "baseline_avg": float(r[4]),
                "baseline_posts": int(r[5]),
                "momentum_ratio": float(r[6]),
            }
            for r in rows
        ]
        profile = {
            r[0]: {
                "dominant_domain": r[1],
                "dominant_domain_posts": int(r[2] or 0),
                "avg_engagement_score": (
                    float(r[3]) if r[3] is not None else None
                ),
            }
            for r in db.execute(
                "SELECT creator_id, dominant_domain, dominant_domain_posts, "
                "avg_engagement_score FROM v_creator_profile"
            ).fetchall()
        }
        topics: dict[int, list[dict]] = {}
        for r in db.execute(
            "SELECT creator_id, topic, post_count, perf_score, perf_rank, "
            "count_rank FROM v_creator_topics"
        ).fetchall():
            topics.setdefault(int(r[0]), []).append(
                {
                    "topic": str(r[1]),
                    "post_count": int(r[2] or 0),
                    "perf_score": float(r[3]) if r[3] is not None else None,
                    "perf_rank": int(r[4]),
                    "count_rank": int(r[5]),
                }
            )
        for row in result:
            p = profile.get(row["creator_id"], {})
            row["dominant_domain"] = p.get("dominant_domain")
            row["dominant_domain_posts"] = p.get("dominant_domain_posts", 0)
            creator_topics = topics.get(row["creator_id"], [])
            row["topics_by_count"] = [
                t for t in creator_topics if t["count_rank"] <= 5
            ]
            row["topics_by_perf"] = [
                t for t in creator_topics if t["perf_rank"] <= 5
            ]
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
        metrics_row = db.execute(
            "SELECT total_posts, avg_likes, avg_engagement_score, "
            "dominant_domain, dominant_domain_posts, momentum_ratio, "
            "is_rising, standout_count, hot_count FROM v_creator_profile "
            "WHERE creator_id = ?",
            [creator_id],
        ).fetchone()
        creator["metrics"] = (
            {
                "total_posts": int(metrics_row[0] or 0),
                "avg_likes": float(metrics_row[1]) if metrics_row[1] is not None else None,
                "avg_engagement_score": (
                    float(metrics_row[2]) if metrics_row[2] is not None else None
                ),
                "dominant_domain": metrics_row[3],
                "dominant_domain_posts": int(metrics_row[4] or 0),
                "momentum_ratio": (
                    float(metrics_row[5]) if metrics_row[5] is not None else None
                ),
                "is_rising": bool(metrics_row[6]),
                "standout_count": int(metrics_row[7] or 0),
                "hot_count": int(metrics_row[8] or 0),
            }
            if metrics_row
            else None
        )
        profile_metrics = {
            r[0]: r[1]
            for r in db.execute(
                "SELECT owner_username, post_count FROM v_profile_metrics"
            ).fetchall()
        }
        profiles_out = []
        for p in creator["profiles"]:
            profile = dict(p)
            profile["post_count"] = (
                profile_metrics.get(p["handle"], 0)
                if p["platform"] == "instagram"
                else 0
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



@app.get("/api/creators/{creator_id}/topics")
def creator_topics(creator_id: int):
    """Per-creator topics — thin projector over ``v_creator_topics``.

    Returns every view row for the creator (≤10); clients slice
    ``count_rank <= 5`` / ``perf_rank <= 5``. No aggregation here.
    """
    db = _connect()
    try:
        rows = db.execute(
            "SELECT topic, post_count, perf_score, perf_rank, count_rank "
            "FROM v_creator_topics WHERE creator_id = ? "
            "ORDER BY count_rank ASC, perf_rank ASC",
            [creator_id],
        ).fetchall()
        return [
            {
                "topic": str(r[0]),
                "post_count": int(r[1] or 0),
                "perf_score": float(r[2]) if r[2] is not None else None,
                "perf_rank": int(r[3]),
                "count_rank": int(r[4]),
            }
            for r in rows
        ]
    finally:
        db.close()


@app.get("/api/creators/{creator_id}/posts")
def creator_posts(creator_id: int):
    creator = get_creator(_ops_resource(), creator_id)
    if creator is None:
        raise HTTPException(status_code=404, detail="Creator not found")
    handles = [
        p["handle"] for p in creator["profiles"] if p["platform"] == "instagram"
    ]
    if not handles:
        return []
    db = _connect()
    try:
        placeholders = ",".join("?" for _ in handles)
        rows = db.execute(
            f"{_POST_SELECT} WHERE v.owner_username IN ({placeholders}) "
            "ORDER BY v.timestamp DESC",
            handles,
        ).fetchall()
        posts = _rows_to_posts(rows)
        return posts
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
