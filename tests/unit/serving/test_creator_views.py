"""Tests for the per-creator serving views ``v_creator_quality`` and
``v_rising_creators`` (replacing the per-profile quality view).

Runs against an in-memory DuckDB with a stub ``v_post_detail`` view carrying
exactly the columns the two views read, then executes the same CREATE VIEW
statements the assets use. Timestamps are relative to ``CURRENT_DATE`` because
the rising-creators view buckets posts into 28/84-day windows off the session
date.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

# ── SQL under test (verbatim from the serving assets) ───────────────────────

CREATOR_QUALITY_SQL = """
CREATE OR REPLACE VIEW v_creator_quality AS
WITH base AS (
    SELECT
        creator_id,
        MAX(creator_name) AS creator_name,
        COUNT(*) AS total_posts,
        COUNT(result_json) AS enriched_posts,
        AVG(CASE WHEN admiralty LIKE 'A%' THEN 3.0
                 WHEN admiralty LIKE 'B%' THEN 2.0
                 WHEN admiralty LIKE 'C%' THEN 1.0
                 WHEN admiralty LIKE 'D%' THEN 0.0 END) AS admiralty_score,
        AVG(CASE WHEN is_educational THEN 1.0
                 WHEN NOT is_educational THEN 0.0 END) AS educational_rate,
        AVG(CASE WHEN is_actionable THEN 1.0
                 WHEN NOT is_actionable THEN 0.0 END) AS actionable_rate,
        AVG(likes_count) AS avg_likes,
        MAX(likes_count) AS max_likes
    FROM v_post_detail
    WHERE creator_id IS NOT NULL
    GROUP BY creator_id
)
SELECT b.*,
       ROUND(0.4 * PERCENT_RANK() OVER (ORDER BY COALESCE(b.admiralty_score, 0))
           + 0.4 * PERCENT_RANK() OVER (ORDER BY COALESCE(LN(1 + b.avg_likes), 0))
           + 0.2 * PERCENT_RANK() OVER (
               ORDER BY b.enriched_posts::DOUBLE / NULLIF(b.total_posts, 0))
           , 4) AS composite_score
FROM base b
WHERE b.enriched_posts >= 3
"""

RISING_CREATORS_SQL = """
CREATE OR REPLACE VIEW v_rising_creators AS
WITH windows AS (
    SELECT
        creator_id,
        MAX(creator_name) AS creator_name,
        AVG(likes_count) FILTER
            (WHERE timestamp >= CURRENT_DATE - INTERVAL '28' DAY) AS recent_avg,
        COUNT(likes_count) FILTER
            (WHERE timestamp >= CURRENT_DATE - INTERVAL '28' DAY) AS recent_posts,
        AVG(likes_count) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '84' DAY
                                 AND  timestamp <  CURRENT_DATE - INTERVAL '28' DAY)
            AS baseline_avg,
        COUNT(likes_count) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '84' DAY
                                   AND  timestamp <  CURRENT_DATE - INTERVAL '28' DAY)
            AS baseline_posts
    FROM v_post_detail
    WHERE creator_id IS NOT NULL
    GROUP BY creator_id
)
SELECT *, recent_avg / NULLIF(baseline_avg, 0) AS momentum_ratio
FROM windows
WHERE recent_posts >= 3
  AND baseline_posts >= 3
  AND baseline_avg > 0
  AND recent_avg >= 5.0
  AND recent_avg / baseline_avg >= 1.25
"""

# ── Fixture plumbing ─────────────────────────────────────────────────────────

# Row shape: (creator_id, creator_name, owner_id, result_json, admiralty,
#              is_educational, is_actionable, likes_count, timestamp)
STUB_DDL = """
CREATE TABLE stub_posts (
    creator_id BIGINT,
    creator_name VARCHAR,
    owner_id VARCHAR,
    result_json VARCHAR,
    admiralty VARCHAR,
    is_educational BOOLEAN,
    is_actionable BOOLEAN,
    likes_count BIGINT,
    timestamp TIMESTAMP
)
"""

INSERT_SQL = (
    "INSERT INTO stub_posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

ENRICHED = '{"ok": true}'


def _days_ago(n: int) -> str:
    """ISO date for ``n`` days before today (midnight, session-relative)."""
    return (dt.date.today() - dt.timedelta(days=n)).isoformat()


def _build(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB seeded with ``rows`` plus both serving views."""
    con = duckdb.connect(":memory:")
    con.execute(STUB_DDL)
    con.executemany(INSERT_SQL, rows)
    con.execute("CREATE OR REPLACE VIEW v_post_detail AS SELECT * FROM stub_posts")
    con.execute(CREATOR_QUALITY_SQL)
    con.execute(RISING_CREATORS_SQL)
    return con


def _enriched_post(
    creator_id: int,
    name: str,
    owner: str,
    likes: int,
    days_ago: int,
    *,
    admiralty: str = "A1",
    is_educational: bool = True,
    is_actionable: bool = True,
) -> tuple:
    return (
        creator_id, name, owner, ENRICHED, admiralty,
        is_educational, is_actionable, likes, _days_ago(days_ago),
    )


def _plain_post(creator_id: int, name: str, owner: str, likes: int, days_ago: int) -> tuple:
    """Non-enriched post (NULL result_json and NULL enrichment flags)."""
    return (creator_id, name, owner, None, None, None, None, likes, _days_ago(days_ago))


# ── v_creator_quality ───────────────────────────────────────────────────────


def test_creator_pooled_across_profiles_single_row():
    """GIVEN posts from 2 different owner_id profiles share a creator_id
    WHEN v_creator_quality runs
    THEN one row with pooled metrics and summed counts.
    """
    con = _build(
        [
            _enriched_post(1, "alice", "owner_a", 10, 1),
            _enriched_post(1, "alice", "owner_a", 20, 2),
            _enriched_post(1, "bob", "owner_b", 30, 3),
        ]
    )
    rows = con.execute(
        "SELECT creator_id, creator_name, total_posts, enriched_posts, avg_likes, max_likes "
        "FROM v_creator_quality WHERE creator_id = 1"
    ).fetchall()
    assert len(rows) == 1
    creator_id, creator_name, total_posts, enriched_posts, avg_likes, max_likes = rows[0]
    assert creator_id == 1
    assert creator_name == "bob"  # MAX(creator_name) pooled across profiles
    assert total_posts == 3
    assert enriched_posts == 3
    assert avg_likes == pytest.approx(20.0)  # (10 + 20 + 30) / 3
    assert max_likes == 30


def test_total_counts_all_posts_enriched_only_non_null():
    """GIVEN a mix of enriched and non-enriched posts
    THEN total_posts counts everything while enriched_posts counts only
    non-NULL result_json, and avg_likes pools over ALL posts.
    """
    con = _build(
        [
            _enriched_post(3, "three", "owner_a", 10, 1),
            _enriched_post(3, "three", "owner_a", 20, 2),
            _enriched_post(3, "three", "owner_a", 30, 3),
            _plain_post(3, "three", "owner_a", 40, 4),
            _plain_post(3, "three", "owner_a", 50, 5),
        ]
    )
    total_posts, enriched_posts, avg_likes = con.execute(
        "SELECT total_posts, enriched_posts, avg_likes "
        "FROM v_creator_quality WHERE creator_id = 3"
    ).fetchone()
    assert total_posts == 5
    assert enriched_posts == 3
    assert avg_likes == pytest.approx(30.0)  # (10+20+30+40+50) / 5


def test_actionable_rate_reflects_flags_educational_unaffected():
    """GIVEN is_actionable varies (incl. NULL) while is_educational is constant
    THEN actionable_rate reflects the true/false split (NULL excluded) and
    educational_rate is unaffected.
    """
    rows = [
        _enriched_post(2, "two", f"owner_{i}", 10 * (i + 1), i + 1,
                       is_actionable=actionable)
        for i, actionable in enumerate([True, False, True, False, None])
    ]
    con = _build(rows)
    actionable_rate, educational_rate = con.execute(
        "SELECT actionable_rate, educational_rate "
        "FROM v_creator_quality WHERE creator_id = 2"
    ).fetchone()
    assert actionable_rate == pytest.approx(0.5)  # 2 of 4 non-NULL actionable
    assert educational_rate == pytest.approx(1.0)  # all 5 educational


def test_creator_with_fewer_than_3_enriched_absent():
    """GIVEN a creator with < 3 enriched posts (and one with none)
    THEN neither appears in v_creator_quality.
    """
    con = _build(
        [
            _enriched_post(4, "four", "owner_a", 10, 1),
            _enriched_post(4, "four", "owner_a", 20, 2),
            _plain_post(4, "four", "owner_a", 30, 3),
            _plain_post(4, "four", "owner_a", 40, 4),
            _plain_post(44, "none", "owner_a", 10, 1),
            _plain_post(44, "none", "owner_a", 20, 2),
            _plain_post(44, "none", "owner_a", 30, 3),
        ]
    )
    assert con.execute(
        "SELECT COUNT(*) FROM v_creator_quality WHERE creator_id IN (4, 44)"
    ).fetchone()[0] == 0


def test_rate_denominator_is_enriched_only():
    """GIVEN 3 enriched 'A1' posts among 100 total
    THEN admiralty_score is 3.0 — averaged over enriched posts only, NOT
    diluted by the 97 non-enriched rows (which would give ~0.09).
    """
    rows = [_enriched_post(5, "five", f"owner_{i}", 10 + i, i + 1) for i in range(3)]
    rows += [_plain_post(5, "five", f"plain_{i}", 0, 10 + i) for i in range(97)]
    con = _build(rows)
    total_posts, enriched_posts, admiralty_score, educational_rate, actionable_rate = con.execute(
        "SELECT total_posts, enriched_posts, admiralty_score, educational_rate, actionable_rate "
        "FROM v_creator_quality WHERE creator_id = 5"
    ).fetchone()
    assert total_posts == 100
    assert enriched_posts == 3
    assert admiralty_score == pytest.approx(3.0)
    assert educational_rate == pytest.approx(1.0)
    assert actionable_rate == pytest.approx(1.0)


# ── v_rising_creators ───────────────────────────────────────────────────────


def test_rising_threshold_125_included_124_excluded():
    """GIVEN a creator whose recent/baseline ratio is exactly 1.25 and one at 1.24
    THEN the 1.25 creator is included, the 1.24 creator is not (all other
    floors met: >= 3 posts per window, baseline_avg > 0, recent_avg >= 5).
    """
    at_125 = [
        _enriched_post(61, "ratio125", f"r_{i}", 25, i + 1) for i in range(4)  # recent
    ] + [
        _enriched_post(61, "ratio125", f"b_{i}", 20, 30 + i) for i in range(4)  # baseline
    ]
    at_124 = [
        _enriched_post(62, "ratio124", f"r_{i}", 31, i + 1) for i in range(4)
    ] + [
        _enriched_post(62, "ratio124", f"b_{i}", 25, 30 + i) for i in range(4)
    ]
    con = _build(at_125 + at_124)

    row = con.execute(
        "SELECT recent_avg, recent_posts, baseline_avg, baseline_posts, momentum_ratio "
        "FROM v_rising_creators WHERE creator_id = 61"
    ).fetchone()
    assert row is not None
    recent_avg, recent_posts, baseline_avg, baseline_posts, momentum_ratio = row
    assert recent_posts == 4
    assert baseline_posts == 4
    assert recent_avg == pytest.approx(25.0)
    assert baseline_avg == pytest.approx(20.0)
    assert momentum_ratio == pytest.approx(1.25)

    assert con.execute(
        "SELECT COUNT(*) FROM v_rising_creators WHERE creator_id = 62"
    ).fetchone()[0] == 0


def test_rising_zero_baseline_excluded():
    """GIVEN baseline posts exist but all have 0 likes
    THEN baseline_avg == 0 and the creator is excluded."""
    con = _build(
        [_enriched_post(7, "zero", f"r_{i}", 10, i + 1) for i in range(3)]
        + [_enriched_post(7, "zero", f"b_{i}", 0, 30 + i) for i in range(3)]
    )
    assert con.execute(
        "SELECT COUNT(*) FROM v_rising_creators WHERE creator_id = 7"
    ).fetchone()[0] == 0


def test_rising_fewer_than_3_posts_in_window_excluded():
    """GIVEN a creator with < 3 posts in either window
    THEN they are excluded (recent side and baseline side).
    """
    short_recent = (
        [_enriched_post(81, "short_recent", f"r_{i}", 10, i + 1) for i in range(2)]
        + [_enriched_post(81, "short_recent", f"b_{i}", 8, 30 + i) for i in range(3)]
    )
    short_baseline = (
        [_enriched_post(82, "short_baseline", f"r_{i}", 10, i + 1) for i in range(3)]
        + [_enriched_post(82, "short_baseline", f"b_{i}", 8, 30 + i) for i in range(2)]
    )
    con = _build(short_recent + short_baseline)
    assert con.execute(
        "SELECT COUNT(*) FROM v_rising_creators WHERE creator_id IN (81, 82)"
    ).fetchone()[0] == 0


def test_rising_null_timestamp_excluded_from_windows():
    """GIVEN a post with NULL timestamp and huge likes
    THEN it is excluded from both window averages and counts.
    """
    rows = [_enriched_post(9, "nine", f"r_{i}", 10, i + 1) for i in range(3)]
    rows += [_enriched_post(9, "nine", f"b_{i}", 8, 30 + i) for i in range(3)]
    rows.append((9, "nine", "no_ts", ENRICHED, "A1", True, True, 1000, None))
    con = _build(rows)

    row = con.execute(
        "SELECT recent_avg, recent_posts, baseline_avg, baseline_posts, momentum_ratio "
        "FROM v_rising_creators WHERE creator_id = 9"
    ).fetchone()
    assert row is not None
    recent_avg, recent_posts, baseline_avg, baseline_posts, momentum_ratio = row
    assert recent_posts == 3
    assert baseline_posts == 3
    assert recent_avg == pytest.approx(10.0)  # 1000-like NULL-ts row excluded
    assert baseline_avg == pytest.approx(8.0)
    assert momentum_ratio == pytest.approx(1.25)
