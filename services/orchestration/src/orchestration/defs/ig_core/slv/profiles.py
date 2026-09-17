"""`silver_ig_profiles` — profile observations plus avatar fetch."""

import logging
from datetime import UTC, datetime

import polars as pl
from dagster import asset

from orchestration.defs.ig_core.slv.posts import (
    _classify_bronze,
    _read_downloaded_at,
)
from orchestration.defs.ig_core.slv.posts import (
    ensure_state_tables as _ensure_state_tables,
)
from orchestration.defs.platform.paths import BRONZE_LAKE
from orchestration.defs.platform.resources import (
    DuckDBResource,
    SQLiteResource,
)

logger = logging.getLogger(__name__)

_PROFILE_OBS_COLUMNS: tuple[str, ...] = (
    "owner_id",
    "owner_username",
    "observed_at",
    "followers_count",
    "follows_count",
    "posts_count",
    "is_verified",
    "source_dataset",
)


def _profile_observations(
    df: pl.DataFrame, entity_type: str, dataset_id: str, observed_at: datetime
) -> pl.DataFrame | None:
    """Build follower-observation rows for one bronze file, or ``None``.

    US-A2.1 gate: the file must GENUINELY carry ``followersCount`` — details
    files always do; posts files only when the scrape actor embedded the
    owner object. The gate evaluates the RAW frame (before renames and
    defaults) so a defaulted ``followers_count=0`` can never be recorded as
    an observation — fabricated zeros are why most ``silver_ig_profiles``
    rows sit at 0-100.

    Owner identity follows the entity type: a details row IS the profile
    (``id`` is the profile id); a posts row carries ``ownerId``. Rows with a
    null owner id (failed requests) or null follower count (Apify error
    rows) are dropped; multiple rows per owner in one file collapse to the
    first.

    Returns ``None`` when the gate excludes the file — no observation rows
    are emitted for absent/defaulted follower counts.
    """
    if "followersCount" not in df.columns:
        return None
    id_col = "id" if entity_type == "details" else "ownerId"
    if id_col not in df.columns:
        return None

    def _maybe(col: str, dtype: pl.DataType) -> pl.Expr:
        return (
            pl.col(col).cast(dtype, strict=False)
            if col in df.columns
            else pl.lit(None, dtype=dtype)
        ).alias(col)

    verified_col = next(
        (c for c in ("isVerified", "verified") if c in df.columns), None
    )
    obs = df.select(
        pl.col(id_col).cast(pl.Utf8).alias("owner_id"),
        _maybe("username", pl.Utf8).alias("owner_username"),
        pl.lit(observed_at).alias("observed_at"),
        pl.col("followersCount").cast(pl.Int32, strict=False).alias("followers_count"),
        _maybe("followsCount", pl.Int32).alias("follows_count"),
        _maybe("postsCount", pl.Int32).alias("posts_count"),
        (
            pl.col(verified_col).cast(pl.Boolean, strict=False)
            if verified_col
            else pl.lit(None, dtype=pl.Boolean)
        ).alias("is_verified"),
        pl.lit(dataset_id).alias("source_dataset"),
    )
    obs = obs.filter(
        pl.col("owner_id").is_not_null() & pl.col("followers_count").is_not_null()
    ).unique(subset=["owner_id"], keep="first")
    return obs if not obs.is_empty() else None


@asset(
    name="silver_ig_profiles",
    group_name="instagram",
    description="Extract profiles + download avatars from post/details scrapes.",
    deps=["bronze_ig_posts"],
)
def silver_ig_profiles(duckdb: DuckDBResource, ops: SQLiteResource) -> pl.DataFrame:
    """Extract profiles from post and details scrapes; download avatars.

    Post scrapes carry the author's profile fields (username, profilePicUrlHD,
    owner_id) on every row, so profiles can be built from either entity type.
    Avatars are downloaded at scrape time — CDN URLs expire in ~4-5 days.
    """

    from orchestration.defs.ig_core.slv.roster import enabled_profiles
    from orchestration.defs.platform.paths import avatar_path
    from orchestration.defs.platform.schemas import DUCKDB_TABLES

    db = duckdb
    _ensure_state_tables(db)

    # Profile list comes from the PUBLISHED roster (`silver_ig_roster`), which
    # the dashboard owns and serves. Reading ops.sqlite here would put the
    # pipeline and the dashboard on one database again.
    with db.get_connection() as conn:
        targets = enabled_profiles(conn)
    if targets:
        logger.info(
            "Tracking %d enabled profile(s): %s",
            len(targets),
            sorted(t["handle"] for t in targets),
        )

    # Find details-type bronze files that haven't been processed
    bronze_files = sorted(BRONZE_LAKE.glob("*.parquet"))
    if not bronze_files:
        if targets:
            logger.warning("No bronze files for %d enabled target(s)", len(targets))
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    with db.get_connection() as conn:
        row = conn.execute("SELECT timestamp FROM watermarks WHERE name = 'profiles_ig'").fetchone()
    if row and row[0] is not None:
        dt = row[0]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        watermark_ts = dt.timestamp()
    else:
        watermark_ts = 0.0

    import os as _os

    new_files = [f for f in bronze_files if _os.path.getmtime(f) > watermark_ts]
    if not new_files:
        with db.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM silver_ig_profiles").fetchone()[0]
            if count == 0:
                return pl.DataFrame(schema={"owner_id": pl.Utf8})
            reader = conn.execute(
                "SELECT * FROM silver_ig_profiles ORDER BY owner_username"
            ).arrow()
        return pl.from_arrow(reader.read_all())

    frames = []
    max_mtime = 0.0
    for f in new_files:
        try:
            df = pl.read_parquet(f)
        except Exception as exc:
            logger.warning("Skipping %s — unreadable: %s", f.name, exc)
            continue

        if len(df) == 0:
            logger.info("Skipping %s — 0 rows", f.name)
            continue

        # Classify — process both details-type and post-type (both carry
        # profile fields). Comments carry no profile data.
        meta_path = f.with_suffix(".parquet.meta")
        entity_type = _classify_bronze(df, meta_path)
        if entity_type not in ("details", "posts"):
            continue

        mtime = _os.path.getmtime(f)
        if mtime > max_mtime:
            max_mtime = mtime

        # ── Follower-observation append (US-A2.1) ────────────────────────
        # One row per profile per file, ONLY when this file genuinely
        # carried followersCount (details always; posts when the actor
        # embedded the owner object). observed_at provenance: meta
        # downloaded_at → file mtime. INSERT OR IGNORE on PK
        # (owner_id, observed_at, source_dataset) keeps reprocessing a
        # file a no-op; a re-scrape under a new dataset appends exactly
        # one observation per profile.
        observed_at = _read_downloaded_at(meta_path) or datetime.fromtimestamp(
            mtime, tz=UTC
        )
        obs = _profile_observations(df, entity_type, f.stem, observed_at)
        if obs is not None:
            with db.get_connection() as conn:
                conn.register("profile_obs_new", obs.to_arrow())
                conn.execute(
                    "INSERT OR IGNORE INTO silver_ig_profile_observations "
                    "SELECT owner_id, owner_username, observed_at, "
                    "followers_count, follows_count, posts_count, "
                    "is_verified, source_dataset FROM profile_obs_new"
                )
                appended = conn.execute(
                    "SELECT COUNT(*) FROM profile_obs_new"
                ).fetchone()[0]
            logger.info(
                "Profile observations from %s: %d rows (observed_at=%s)",
                f.name,
                appended,
                observed_at,
            )

        # Map camelCase columns → snake_case
        _profile_col_map = {
            "ownerId": "owner_id",
            "username": "owner_username",
            "fullName": "full_name",
            "biography": "biography",
            "followersCount": "followers_count",
            "followsCount": "follows_count",
            "postsCount": "posts_count",
            "isBusinessAccount": "is_business",
            "isVerified": "is_verified",
            "externalUrl": "external_url",
        }
        to_rename = {
            old: new
            for old, new in _profile_col_map.items()
            if old in df.columns and new not in df.columns
        }
        df = df.rename(to_rename)

        # Profile pic: prefer HD, fall back to standard. Handled separately
        # because both source columns map to the same target.
        if "profilePicUrlHD" in df.columns:
            df = df.rename({"profilePicUrlHD": "profile_pic_url"})
        elif "profilePicUrl" in df.columns:
            df = df.rename({"profilePicUrl": "profile_pic_url"})

        dataset_id = f.stem
        for col, default in [
            ("owner_id", None),
            ("owner_username", None),
            ("full_name", None),
            ("biography", None),
            ("followers_count", 0),
            ("follows_count", 0),
            ("posts_count", 0),
            ("is_business", False),
            ("is_verified", False),
            ("profile_pic_url", None),
            ("external_url", None),
            ("source_dataset", dataset_id),
            ("processed_on", None),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

        # Download avatar from fresh CDN URL (they expire in ~4-5 days)
        if "profile_pic_url" in df.columns:
            profile_rows = df.select(["owner_username", "profile_pic_url"]).unique().rows()
            for owner_username, pic_url in profile_rows:
                if not owner_username or not pic_url:
                    continue
                local = avatar_path(owner_username)
                if local.exists() and local.stat().st_size > 0:
                    continue  # Already cached
                try:
                    import urllib.request as _urllib

                    req = _urllib.Request(
                        pic_url,
                        headers={
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/120.0.0.0 Safari/537.36"
                            ),
                            "Referer": "https://www.instagram.com/",
                        },
                    )
                    resp = _urllib.urlopen(req, timeout=15)
                    body = resp.read()
                    local.write_bytes(body)
                    logger.info(
                        "Downloaded avatar %s -> %s (%d bytes)",
                        owner_username,
                        local,
                        len(body),
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to download avatar for %s: %s",
                        owner_username,
                        exc,
                    )
                    continue

        # Keep only valid profile columns
        schema_cols = list(DUCKDB_TABLES["silver_ig_profiles"].keys())
        df = df.select([c for c in schema_cols if c in df.columns])

        # Drop rows without owner_id, then collapse post scrapes (one row per
        # post) to a single row per profile.
        df = df.filter(pl.col("owner_id").is_not_null())
        df = df.unique(subset=["owner_id"], keep="first")
        frames.append(df)

    # ── 3. Load existing + dedup via DuckDB ─────────────────────────────
    with db.get_connection() as conn:
        existing_count = conn.execute("SELECT COUNT(*) FROM silver_ig_profiles").fetchone()[0]

    if existing_count > 0:
        with db.get_connection() as conn:
            reader = conn.execute("SELECT * FROM silver_ig_profiles").arrow()
        existing_df = pl.from_arrow(reader.read_all())
        frames.insert(0, existing_df)

    if not frames:
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    unified = pl.concat(frames, how="diagonal_relaxed")
    if unified.is_empty():
        return pl.DataFrame(schema={"owner_id": pl.Utf8})

    now_iso = datetime.now(UTC).isoformat()
    unified = unified.with_columns(
        pl.when(pl.col("processed_on").is_null())
        .then(pl.lit(now_iso))
        .otherwise(pl.col("processed_on"))
        .alias("processed_on")
    )

    with db.get_connection() as conn:
        conn.register("to_upsert", unified.to_arrow())
        conn.execute("INSERT OR REPLACE INTO silver_ig_profiles SELECT * FROM to_upsert")

    # Advance watermark
    if max_mtime > 0:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES ('profiles_ig', ?)",
                [datetime.fromtimestamp(max_mtime, tz=UTC).replace(tzinfo=None)],
            )

    return unified
