"""
FastAPI data server for the Lakehouse dashboard.
Connects to DuckDB and exposes analytics views as JSON endpoints.
Run with: uv run uvicorn server:app --port 3002 --reload
"""

from __future__ import annotations

import logging
import re
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard-api")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "state.duckdb"
OPS_PATH = Path(__file__).resolve().parent.parent / "data" / "ops.sqlite"

app = FastAPI(title="Lakehouse Dashboard API")

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

def _ensure_media_cache():
    """Idempotent schema creation for instagram media cache."""
    con = _ops_connect()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS instagram_media_cache (
                cache_key   TEXT PRIMARY KEY,
                media_url   TEXT NOT NULL,
                media_type  TEXT NOT NULL,
                fetched_at  TEXT NOT NULL,
                error       TEXT
            )
        """)
        con.commit()
    finally:
        con.close()




_ensure_media_cache()


def _fetch_og_image(url: str) -> str | None:
    """Scrape a URL and extract og:image meta tag. Returns None on failure."""
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        resp = urllib.request.urlopen(req, timeout=8)
        html = resp.read().decode("utf-8", errors="replace")
        match = re.search(
            r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html
        )
        if match:
            return match.group(1)
    except Exception as exc:
        logger.warning("Failed to fetch og:image from %s: %s", url, exc)
    return None


def _get_cached_media(cache_key: str) -> dict | None:
    """Return cached media entry or None."""
    con = _ops_connect()
    try:
        row = con.execute(
            "SELECT media_url, media_type, fetched_at, error "
            "FROM instagram_media_cache WHERE cache_key = ?",
            [cache_key],
        ).fetchone()
        if row:
            return dict(row)
    finally:
        con.close()
    return None


def _cache_media(cache_key: str, media_url: str, media_type: str) -> None:
    """Store a media URL in the cache."""
    con = _ops_connect()
    try:
        now = datetime.now(timezone.utc).isoformat()
        con.execute(
            "INSERT OR REPLACE INTO instagram_media_cache "
            "(cache_key, media_url, media_type, fetched_at) VALUES (?, ?, ?, ?)",
            [cache_key, media_url, media_type, now],
        )
        con.commit()
    finally:
        con.close()


@app.get("/api/media/avatar/{username}")
def avatar(username: str):
    """Get profile picture URL for an Instagram user. Cached after first fetch."""
    cache_key = f"avatar:{username}"
    cached = _get_cached_media(cache_key)
    if cached and not cached.get("error"):
        return {"url": cached["media_url"], "cached": True}

    # Fetch from Instagram profile page
    url = _fetch_og_image(f"https://www.instagram.com/{username}/")
    if url:
        _cache_media(cache_key, url, "avatar")
        return {"url": url, "cached": False}

    # Fallback: DiceBear identicon
    fallback = (
        f"https://api.dicebear.com/9.x/identicon/svg"
        f"?seed={username}&backgroundColor=000000&foregroundColor=00ffff"
    )
    _cache_media(cache_key, fallback, "avatar")
    return {"url": fallback, "cached": False, "fallback": True}


@app.get("/api/media/thumbnail/{shortcode}")
def thumbnail(shortcode: str):
    """Get post thumbnail URL. Lazy-loaded: fetched on first request, cached after."""
    cache_key = f"thumb:{shortcode}"
    cached = _get_cached_media(cache_key)
    if cached and not cached.get("error"):
        return {"url": cached["media_url"], "cached": True}

    # Fetch from Instagram post page
    post_url = f"https://www.instagram.com/p/{shortcode}/"
    url = _fetch_og_image(post_url)
    if url:
        _cache_media(cache_key, url, "thumbnail")
        return {"url": url, "cached": False}

    # Return empty — frontend will show placeholder
    return {"url": None, "cached": False}


# ── Health ──────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── Overview Metrics ───────────────────────────────────────────

@app.get("/api/overview")
def overview():
    db = _connect()
    try:
        total_posts = db.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0]

        total_enriched = db.execute(
            "SELECT COUNT(*) FROM gold_analyses WHERE domain = 'instagram'"
        ).fetchone()[0]

        total_profiles = db.execute(
            "SELECT COUNT(DISTINCT owner_username) FROM silver_ig_posts"
        ).fetchone()[0]

        enrichment_pct = (
            round((total_enriched / total_posts * 100), 1) if total_posts else 0
        )

        admiralty = (
            db.execute(
                "SELECT ROUND(AVG(admiralty_score), 2) FROM v_profile_quality WHERE enriched_posts > 0"
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
            SELECT owner_id, owner_username, total_posts, enriched_posts,
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
                "total_posts": r[2],
                "enriched_posts": r[3],
                "admiralty_score": float(r[4]) if r[4] else 0,
                "educational_rate": float(r[5]) if r[5] else 0,
                "avg_likes": float(r[6]) if r[6] else 0,
                "avg_comments": float(r[7]) if r[7] else 0,
                "avg_video_views": float(r[8]) if r[8] else 0,
                "max_likes": r[9] or 0,
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
            SELECT post_id, owner_username, admiralty, gold_domain, gold_topic,
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
                "admiralty": r[2],
                "gold_domain": r[3],
                "gold_topic": r[4],
                "is_educational": bool(r[5]) if r[5] is not None else False,
                "is_actionable": bool(r[6]) if r[6] is not None else False,
                "caption": r[7] or "",
                "likes_count": r[8] or 0,
                "comments_count": r[9] or 0,
                "video_view_count": r[10] or 0,
                "shortcode": r[11] or "",
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
            safe_sort = sort if sort in (
                "likes_count", "comments_count", "video_view_count",
                "timestamp", "owner_username", "admiralty",
            ) else "timestamp"
            safe_order = "DESC" if order and order.upper() == "DESC" else "ASC"
            order_clause = f"ORDER BY v.{safe_sort} {safe_order}"

        limit_clause = ""
        if limit > 0:
            limit_clause = f"LIMIT {int(limit)} OFFSET {int(offset)}"

        rows = db.execute(
            f"""
            SELECT v.post_id, v.owner_username, v.caption,
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
                SELECT sp.post_id, sp.owner_username, sp.shortcode,
                       sp.caption, sp.likes_count, sp.comments_count,
                       sp.video_view_count, sp.timestamp,
                       cs.mean_likes, cs.std_likes,
                       ROUND((sp.likes_count - cs.mean_likes) / NULLIF(cs.std_likes, 0), 2) AS z_score
                FROM silver_ig_posts sp
                JOIN creator_stats cs ON sp.owner_username = cs.owner_username
                WHERE sp.likes_count > cs.mean_likes + cs.std_likes
            )
            SELECT * FROM standouts
            ORDER BY z_score DESC
            LIMIT ?
        """, [limit]).fetchall()

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

        return [
            {"day": int(r[0]), "standout_count": r[1]}
            for r in rows
        ]
    finally:
        db.close()


# ── Recent Standouts by Creator ─────────────────────────────────

@app.get("/api/recent-standouts")
def recent_standouts(limit: int = Query(10, ge=1, le=50)):
    """Recent standout posts grouped by creator, for homepage cards."""
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
                SELECT sp.post_id, sp.owner_username, sp.shortcode,
                       sp.caption, sp.likes_count, sp.comments_count,
                       sp.timestamp,
                       cs.mean_likes, cs.std_likes,
                       ROUND((sp.likes_count - cs.mean_likes) / NULLIF(cs.std_likes, 0), 2) AS z_score,
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
        """, [limit]).fetchall()

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3002)
