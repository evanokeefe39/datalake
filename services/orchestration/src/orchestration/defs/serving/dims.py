"""Durable dimensions — SCD2 profile history and the generated calendar."""

from datetime import datetime, timezone

from dagster import AssetKey, asset
from dagster_duckdb import DuckDBResource

from orchestration.defs.platform.resources import SQLiteResource
from orchestration.defs.platform.schemas import duckdb_ddl


@asset(
    name="dim_profile",
    group_name="serving",
    description="SCD2 profile dimension tracking owner attributes over time.",
    deps=[AssetKey("ig_posts_slv")],
)
def profile_dimension(duckdb: DuckDBResource, ops: SQLiteResource) -> None:
    """Upsert profile dimension with SCD2 tracking.

    Reads distinct owner profiles from ``silver_ig_posts`` and maintains
    ``effective_from``/``effective_to``/``is_current`` in DuckDB. ``creator_id``
    and ``creator_name`` are linked from the ``profiles``/``creators`` tables in
    ops.sqlite so every serving view can expose the owning creator.
    """
    from opsdb.roster import creator_map

    db = duckdb
    with db.get_connection() as conn:
        conn.execute(duckdb_ddl("dim_profile"))

        # Migration: existing DBs predate profile_pic_path and the creator
        # columns. DuckDB ALTER has no IF NOT EXISTS, so tolerate the
        # duplicate-column error.
        for col, typ in (
            ("profile_pic_path", "TEXT"),
            ("creator_id", "INTEGER"),
            ("creator_name", "TEXT"),
        ):
            try:
                conn.execute(f"ALTER TABLE dim_profile ADD COLUMN {col} {typ}")
            except Exception:
                pass  # column already exists

        # Get distinct profiles from silver_ig_posts
        profiles = conn.execute("""
            SELECT DISTINCT owner_id, owner_username
            FROM silver_ig_posts
            WHERE owner_id IS NOT NULL
        """).fetchall()

        # Creator link: {handle: {creator_id, creator_name}} from ops.
        handle_map = creator_map(ops)

        if not profiles:
            return

        # Determine next profile_key
        max_key = conn.execute("SELECT COALESCE(MAX(profile_key), 0) FROM dim_profile").fetchone()[
            0
        ]

        now_ts = datetime.now(timezone.utc).isoformat()

        for owner_id, owner_username in profiles:
            creator = handle_map.get(owner_username, {})

            # Check existing current row
            existing = conn.execute(
                """
                SELECT profile_key, owner_username
                FROM dim_profile
                WHERE owner_id = ? AND is_current = TRUE
            """,
                [owner_id],
            ).fetchone()

            if existing:
                existing_key, existing_username = existing
                if existing_username == owner_username:
                    # No identity change — creator link refreshed below.
                    continue
                # Close the old row
                conn.execute(
                    """
                    UPDATE dim_profile
                    SET effective_to = ?, is_current = FALSE
                    WHERE profile_key = ?
                """,
                    [now_ts, existing_key],
                )

            # Insert new row
            max_key += 1
            conn.execute(
                """
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username, channel,
                     effective_from, effective_to, is_current,
                     creator_id, creator_name)
                VALUES (?, ?, ?, 'instagram', ?, NULL, TRUE, ?, ?)
            """,
                [
                    max_key,
                    owner_id,
                    owner_username,
                    now_ts,
                    creator.get("creator_id"),
                    creator.get("creator_name"),
                ],
            )

        # Refresh the creator link on current rows. This is a mutable
        # relationship (a profile's owner), not a slowly-changing attribute,
        # so it updates in place rather than versioning a new SCD2 row.
        for owner_username, creator in handle_map.items():
            conn.execute(
                """
                UPDATE dim_profile
                SET creator_id = ?, creator_name = ?
                WHERE owner_username = ? AND is_current = TRUE
            """,
                [creator["creator_id"], creator["creator_name"], owner_username],
            )


@asset(
    name="dim_date",
    group_name="serving",
    description="Generated date dimension: 1 year back from today, with fiscal year (Jul–Jun).",
)
def dim_date(duckdb: DuckDBResource) -> None:
    """Generate a standard date dimension table.

    One row per day from (CURRENT_DATE - 1 year) through CURRENT_DATE.
    Financial year runs July–June (e.g. FY2026 = Jul 2025 – Jun 2026).
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE dim_date AS
            SELECT
                date_col::DATE                                        AS date,
                EXTRACT(YEAR FROM date_col)                           AS year,
                EXTRACT(QUARTER FROM date_col)                        AS quarter,
                EXTRACT(MONTH FROM date_col)                          AS month_number,
                MONTHNAME(date_col)                                   AS month_name,
                EXTRACT(WEEK FROM date_col)                           AS week_number,
                EXTRACT(DAY FROM date_col)                            AS day_number,
                DAYNAME(date_col)                                     AS day_of_week,
                CASE WHEN DAYOFWEEK(date_col) IN (0, 6)
                     THEN TRUE ELSE FALSE END                         AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM date_col) >= 7
                     THEN EXTRACT(YEAR FROM date_col)
                     ELSE EXTRACT(YEAR FROM date_col) - 1
                END                                                   AS financial_year
            FROM generate_series(
                CURRENT_DATE - INTERVAL 1 YEAR,
                CURRENT_DATE,
                INTERVAL 1 DAY
            ) AS t(date_col)
        """)


ASSETS: list = [profile_dimension, dim_date]
