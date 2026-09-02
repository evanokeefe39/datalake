"""Tests for the creator-metrics serving views (v_post_baselines,
v_post_metrics z/engagement extension, v_creator_profile, v_creator_topics,
v_rising_creators projection).

Contract under test (WATCHDOG semantic contracts, 2026-09-02):

- ``v_post_baselines`` mirrors the label-pass estimator (N=20 trailing priors,
  90-day lookback when < 20 priors, min n=5, center=Q3, spread=IQR) with
  baseline key ``COALESCE(creator_id, owner_username)`` and STRICT priors
  (``q.timestamp < p.timestamp`` — no future leak).
- comments z uses the comments baseline (0 stays in the window, NULL drops);
  views z only exists where the post's own ``video_view_count`` > 0 — image
  posts render NULL ``views_zscore`` (and NULL views_* baseline columns).
- ``engagement_score`` = 0.5·likes_z + 0.3·comments_z + 0.2·views_z with NULL
  components contributing 0; NULL only when ALL three z are NULL.
- Momentum windows/gates are defined once in ``v_creator_profile``;
  ``v_rising_creators`` is exactly its ``is_rising`` projection.
- ``v_creator_topics`` keeps top-5 by count OR by performance (RANK ties share).
"""

from __future__ import annotations

import datetime as dt

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.assets import (
    v_creator_profile as _v_creator_profile,
)
from datalake.defs.serving.assets import (
    v_creator_topics as _v_creator_topics,
)
from datalake.defs.serving.assets import (
    v_post_baselines as _v_post_baselines,
)
from datalake.defs.serving.assets import (
    v_post_metrics as _v_post_metrics,
)
from datalake.defs.serving.assets import (
    v_rising_creators as _v_rising_creators,
)


# ── Fixture plumbing ─────────────────────────────────────────────────────────

POST_COLUMNS = (
    "post_id, owner_username, creator_id, channel, creator_name, "
    "likes_count, comments_count, video_view_count, timestamp, "
    "gold_domain, gold_topic"
)

LABEL_COLUMNS = (
    "post_id, label, method, is_provisional, likes_zscore, sigma_tier, "
    "baseline_center, baseline_spread"
)


def _ts(days_ago: float, hour: int = 12) -> str:
    """ISO timestamp `days_ago` days before now (hour pinned off midnight)."""
    moment = dt.datetime.now() - dt.timedelta(days=days_ago)
    moment = moment.replace(hour=hour, minute=0, second=0, microsecond=0)
    return moment.isoformat(sep=" ")


def _post(
    post_id: str,
    creator_id: int,
    *,
    owner: str | None = None,
    likes: int | None = 10,
    comments: int | None = 0,
    views: int | None = None,
    days_ago: float | None = 30,
    domain: str | None = None,
    topic: str | None = None,
    name: str = "Creator",
) -> tuple:
    """v_post_detail row with sensible defaults (image post by default)."""
    return (
        post_id, owner, creator_id, "instagram", name,
        likes, comments, views, None if days_ago is None else _ts(days_ago),
        domain, topic,
    )


def _label(
    post_id: str,
    *,
    label: str = "average",
    likes_z: float | None = 0.0,
    center: float | None = 100.0,
    spread: float | None = 50.0,
) -> tuple:
    """ig_post_labels row; defaults judge the post as an ordinary one."""
    return (post_id, label, "day7_matched", False, likes_z, "normal",
            center, spread)


def _seed(con, posts: list[tuple], labels: list[tuple]) -> None:
    insert_post = (
        f"INSERT INTO v_post_detail ({POST_COLUMNS})"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for row in posts:
        con.execute(insert_post, row)
    insert_label = (
        f"INSERT INTO ig_post_labels ({LABEL_COLUMNS})"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for row in labels:
        con.execute(insert_label, row)


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """DuckDB resource stubbed with v_post_detail + ig_post_labels upstreams.

    ``v_engagement_outliers`` is stubbed as the same projection the real asset
    builds (v_post_detail .* + label fields) so the assets under test are the
    REAL committed SQL.
    """
    resource = DuckDBResource(database=str(tmp_path / "creator_metrics.duckdb"))
    with resource.get_connection() as con:
        con.execute(f"""
            CREATE TABLE v_post_detail (
                post_id TEXT PRIMARY KEY, owner_username TEXT,
                creator_id INTEGER, channel TEXT, creator_name TEXT,
                likes_count BIGINT, comments_count BIGINT,
                video_view_count BIGINT, timestamp TIMESTAMP,
                shortcode TEXT, caption TEXT,
                gold_domain TEXT, gold_topic TEXT
            )
        """)
        con.execute(f"""
            CREATE TABLE ig_post_labels (
                post_id TEXT PRIMARY KEY, label TEXT, method TEXT,
                is_provisional BOOLEAN, likes_zscore DOUBLE, sigma_tier TEXT,
                baseline_center DOUBLE, baseline_spread DOUBLE
            )
        """)
        con.execute("""
            CREATE VIEW v_engagement_outliers AS
            SELECT d.*, l.label, l.method, l.is_provisional,
                   l.likes_zscore, l.sigma_tier
            FROM v_post_detail d
            LEFT JOIN ig_post_labels l ON d.post_id = l.post_id
        """)
    return resource


def _run_metrics_chain(db: DuckDBResource) -> None:
    """Materialize the five views under test in dependency order."""
    ctx = build_asset_context(resources={"duckdb": db})
    _v_post_baselines(ctx)
    _v_post_metrics(ctx)
    _v_creator_profile(ctx)
    _v_creator_topics(ctx)
    _v_rising_creators(ctx)


# ── v_post_baselines: window semantics ───────────────────────────────────────


def test_baseline_uses_only_strict_priors_no_future_leak(db):
    """A later post must never enter an earlier post's window."""
    with db.get_connection() as con:
        posts = [
            # 5 priors with comments 10..50 (oldest → newest)
            _post("p1", 1, comments=10, days_ago=7),
            _post("p2", 1, comments=20, days_ago=6),
            _post("p3", 1, comments=30, days_ago=5),
            _post("p4", 1, comments=40, days_ago=4),
            _post("p5", 1, comments=50, days_ago=3),
            # target judged at t-2
            _post("target", 1, comments=100, days_ago=2),
            # FUTURE post relative to target — must NOT leak into its window
            _post("future", 1, comments=10000, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3, iqr = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3, comments_baseline_iqr"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 5
        assert q3 == 40.0  # Q3 of [10,20,30,40,50]
        assert iqr == 20.0  # 40 - 20
        z = con.execute(
            "SELECT comments_zscore FROM v_post_metrics WHERE post_id = 'target'"
        ).fetchone()[0]
        # (100 - 40) / 20 — a leaked future value (10000) would blow this up
        assert z == pytest.approx(3.0)


def test_baseline_n20_cap_takes_most_recent_priors(db):
    """With >= 20 priors the window is the 20 most recent, not all priors."""
    with db.get_connection() as con:
        posts = [
            _post(f"p{j}", 1, comments=j + 1, days_ago=j + 1)
            for j in range(25)  # comments 1..25, most recent = 1
        ]
        posts.append(_post("target", 1, comments=5, days_ago=0.5))
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3 = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 20
        # 20 most recent priors are comments 1..20 → Q3 = 1 + 0.75·19 = 15.25
        # (all 25 would give 19.0)
        assert q3 == pytest.approx(15.25)


def test_baseline_90day_expansion_when_under_20_priors(db):
    """With < 20 priors the window keeps only priors within 90 days."""
    with db.get_connection() as con:
        posts = [
            # 4 priors older than 90 days — outside the expanded window
            _post("old1", 1, comments=1000, days_ago=100),
            _post("old2", 1, comments=2000, days_ago=110),
            # 6 priors within 90 days → n=6 >= min n=5
            _post("r1", 1, comments=1, days_ago=60),
            _post("r2", 1, comments=2, days_ago=50),
            _post("r3", 1, comments=3, days_ago=40),
            _post("r4", 1, comments=4, days_ago=30),
            _post("r5", 1, comments=5, days_ago=20),
            _post("r6", 1, comments=6, days_ago=10),
            _post("target", 1, comments=3, days_ago=5),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3, iqr = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3, comments_baseline_iqr"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 6  # old priors excluded by the 90-day lookback
        assert q3 == pytest.approx(4.75)  # Q3 of [1..6]
        assert iqr == pytest.approx(2.5)  # 4.75 - 2.25


def test_baseline_min_n_5_below_gate_is_null(db):
    """Fewer than 5 usable priors → NULL baseline, NULL z (not a low-n guess)."""
    with db.get_connection() as con:
        posts = [
            _post("c1", 1, comments=10, days_ago=5),
            _post("c2", 1, comments=20, days_ago=4),
            _post("c3", 1, comments=30, days_ago=3),
            _post("c4", 1, comments=40, days_ago=2),
            _post("target", 1, comments=999, days_ago=1, likes=500),
        ]
        _seed(con, posts, [_label("target", likes_z=4.0)])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3, iqr = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3, comments_baseline_iqr"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 4  # true window size, mirrors ig_post_labels.baseline_n
        assert q3 is None and iqr is None
        cz, ez = con.execute(
            "SELECT comments_zscore, engagement_score"
            " FROM v_post_metrics WHERE post_id = 'target'"
        ).fetchone()
        assert cz is None
        # engagement falls back to the likes component alone
        assert ez == pytest.approx(2.0)  # 0.5 · 4.0


def test_baseline_pools_profiles_by_creator_id(db):
    """Priors from two profiles of the same creator_id share one window."""
    with db.get_connection() as con:
        posts = [
            _post("a1", 1, owner="alice", comments=10, days_ago=6),
            _post("a2", 1, owner="alice_2", comments=20, days_ago=5),
            _post("a3", 1, owner="alice", comments=30, days_ago=4),
            _post("a4", 1, owner="alice_2", comments=40, days_ago=3),
            _post("a5", 1, owner="alice", comments=50, days_ago=2),
            _post("target", 1, owner="alice", comments=100, days_ago=1),
            # a different creator's posts must NOT enter the window
            _post("b1", 2, owner="bob", comments=900, days_ago=3),
            _post("b2", 2, owner="bob", comments=900, days_ago=2),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3 = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 5  # a1..a5 pooled across both handles
        assert q3 == pytest.approx(40.0)


def test_baseline_key_falls_back_to_owner_username(db):
    """Posts without a creator link still get baselines keyed by handle."""
    with db.get_connection() as con:
        posts = [
            _post("u1", None, owner="solo", comments=10, days_ago=6),
            _post("u2", None, owner="solo", comments=20, days_ago=5),
            _post("u3", None, owner="solo", comments=30, days_ago=4),
            _post("u4", None, owner="solo", comments=40, days_ago=3),
            _post("u5", None, owner="solo", comments=50, days_ago=2),
            _post("target", None, owner="solo", comments=60, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3 = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 5
        assert q3 == pytest.approx(40.0)


def test_null_timestamp_posts_are_excluded_from_windows(db):
    """A prior with NULL timestamp must not enter any window."""
    with db.get_connection() as con:
        posts = [
            _post("t1", 1, comments=10, days_ago=6),
            _post("t2", 1, comments=20, days_ago=5),
            _post("t3", 1, comments=30, days_ago=4),
            _post("t4", 1, comments=40, days_ago=3),
            _post("t5", 1, comments=50, days_ago=2),
            _post("nots", 1, comments=9999, days_ago=None),  # NULL timestamp
            _post("target", 1, comments=100, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3 = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 5  # 'nots' (9999) excluded
        assert q3 == pytest.approx(40.0)


def test_window_tie_broken_deterministically_by_post_id(db):
    """Timestamp ties must not make the N=20 window nondeterministic:
    the greater post_id wins the last window slot (post_id DESC)."""
    with db.get_connection() as con:
        posts = [
            # 19 distinct priors, comments 1..19 (oldest = 19)
            *(_post(f"d{j:02d}", 1, comments=j, days_ago=21 - j)
              for j in range(1, 20)),
            # tied timestamps at the window boundary (recency 20 vs 21):
            # 'b-tie' (greater post_id) must win the last slot with value 15
            _post("a-tie", 1, comments=999, days_ago=21),
            _post("b-tie", 1, comments=15, days_ago=21),
            _post("target", 1, comments=10, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3 = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        assert n == 20
        # window = [1..19, 15] → sorted idx 14.25 lands on the duplicated 15.
        # 'a-tie' (999) winning instead would give 15.25.
        assert q3 == pytest.approx(15.0)


# ── z-scores + engagement_score ──────────────────────────────────────────────


def test_comments_z_zero_stays_in_window_null_drops(db):
    """comments_count 0 stays in the window; NULL comments drop out."""
    with db.get_connection() as con:
        posts = [
            _post("z1", 1, comments=0, days_ago=7),
            _post("z2", 1, comments=0, days_ago=6),
            _post("z3", 1, comments=30, days_ago=5),
            _post("z4", 1, comments=40, days_ago=4),
            _post("z5", 1, comments=None, days_ago=3),  # absent — drops
            _post("z6", 1, comments=50, days_ago=2),
            _post("target", 1, comments=0, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        n, q3, iqr = con.execute(
            "SELECT comments_baseline_n, comments_baseline_q3, comments_baseline_iqr"
            " FROM v_post_baselines WHERE post_id = 'target'"
        ).fetchone()
        # window = [0, 0, 30, 40, 50] — NULL dropped, zeros kept
        assert n == 5
        assert q3 == pytest.approx(40.0)
        assert iqr == pytest.approx(40.0)
        z = con.execute(
            "SELECT comments_zscore FROM v_post_metrics WHERE post_id = 'target'"
        ).fetchone()[0]
        assert z == pytest.approx(-1.0)  # (0 - 40) / 40


def test_views_zscore_null_for_image_and_zero_view_posts(db):
    """Image posts (NULL views) and 0-view videos get NULL views z."""
    with db.get_connection() as con:
        posts = [
            # 5 video priors with views 100..500
            _post("v1", 1, views=100, days_ago=7),
            _post("v2", 1, views=200, days_ago=6),
            _post("v3", 1, views=300, days_ago=5),
            _post("v4", 1, views=400, days_ago=4),
            _post("v5", 1, views=500, days_ago=3),
            # image post: NULL views
            _post("img", 1, views=None, days_ago=2),
            # 0-view video: baseline meaningful only for views > 0
            _post("zero", 1, views=0, days_ago=2),
            # real video target
            _post("vid", 1, views=1000, days_ago=1),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        rows = con.execute(
            "SELECT post_id, views_baseline_n, views_zscore"
            " FROM v_post_metrics WHERE post_id IN ('img', 'zero', 'vid')"
        ).fetchall()
        by_id = {r[0]: (r[1], r[2]) for r in rows}
        # image post: no views baseline at all, no z
        assert by_id["img"] == (None, None)
        # 0-view video: no z (its own views are not > 0)
        assert by_id["zero"][1] is None
        # video target: n=5 window of the 5 priors, z = (1000 - 400)/150
        assert by_id["vid"][0] == 5
        assert by_id["vid"][1] == pytest.approx(3.0)  # (1000 - 400) / 200


def test_engagement_score_weighted_blend_with_null_components(db):
    """0.5·likes + 0.3·comments + 0.2·views; NULL z ⇒ 0; all NULL ⇒ NULL."""
    with db.get_connection() as con:
        posts = [
            # 5 priors with comments 10..50 AND views 100..500 so BOTH
            # baselines exist (n=5) for the target posts
            _post("v1", 1, comments=10, views=100, days_ago=7),
            _post("v2", 1, comments=20, views=200, days_ago=6),
            _post("v3", 1, comments=30, views=300, days_ago=5),
            _post("v4", 1, comments=40, views=400, days_ago=4),
            _post("v5", 1, comments=50, views=500, days_ago=3),
            # full blend: likes_z 4, comments_z 2 ((80-40)/20), views_z 1
            # ((600-400)/200)
            _post("full", 1, comments=80, views=600, days_ago=2, likes=300),
            # image post: views z NULL → only 0.5·likes + 0.3·comments
            _post("img", 1, comments=80, views=None, days_ago=2, likes=300),
            # unlabeled post with no baselines: all z NULL → score NULL
            _post("none", 7, comments=1, views=None, days_ago=2, likes=3),
        ]
        labels = [_label(p[0]) for p in posts[:5]]
        labels += [
            _label("full", likes_z=4.0),
            _label("img", likes_z=4.0),
            # 'none' gets NO label row at all
        ]
        _seed(con, posts, labels)
    _run_metrics_chain(db)

    with db.get_connection() as con:
        scores = dict(
            con.execute(
                "SELECT post_id, engagement_score FROM v_post_metrics"
                " WHERE post_id IN ('full', 'img', 'none')"
            ).fetchall()
        )
        assert scores["full"] == pytest.approx(2.8)  # 0.5·4 + 0.3·2 + 0.2·1
        assert scores["img"] == pytest.approx(2.6)  # 0.5·4 + 0.3·2 (no 0.2 term)
        assert scores["none"] is None


# ── v_creator_profile: momentum + dominant domain ────────────────────────────


def _momentum_posts(creator_id: int, recent_likes, baseline_likes) -> list[tuple]:
    posts = [
        _post(f"{creator_id}-r{j}", creator_id, likes=likes, days_ago=5 + j)
        for j, likes in enumerate(recent_likes)
    ]
    posts += [
        _post(f"{creator_id}-b{j}", creator_id, likes=likes, days_ago=40 + j)
        for j, likes in enumerate(baseline_likes)
    ]
    return posts


def test_is_rising_gates_match_and_rising_creators_is_exact_projection(db):
    """v_rising_creators == the is_rising rows of v_creator_profile, with the
    1.25 ratio / >=3 posts / >=5.0 recent / baseline > 0 gates."""
    with db.get_connection() as con:
        posts = (
            _momentum_posts(1, [30, 30, 30], [10, 10, 10])  # ratio 3.0 → rising
            + _momentum_posts(2, [12, 12, 12], [10, 10, 10])  # ratio 1.2 → not
            + _momentum_posts(3, [30, 30, 30], [0, 0, 0])  # baseline 0 → not
            + _momentum_posts(4, [30, 30], [10, 10, 10])  # < 3 recent → not
+ _momentum_posts(5, [3, 4, 5], [1, 1, 1])  # recent avg 4.0 < 5.0 → not
        )
        _seed(con, posts, [_label(p[0], center=10, spread=5) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        rising = con.execute(
            "SELECT creator_id FROM v_rising_creators ORDER BY creator_id"
        ).fetchall()
        assert rising == [(1,)]
        profile_rising = con.execute(
            "SELECT creator_id, momentum_ratio FROM v_creator_profile"
            " WHERE is_rising ORDER BY creator_id"
        ).fetchall()
        assert profile_rising == [(1, 3.0)]
        # the projection carries identical momentum values
        joined = con.execute(
            """
            SELECT r.creator_id, r.momentum_ratio, r.recent_avg, r.baseline_avg
            FROM v_rising_creators r
            JOIN v_creator_profile p ON p.creator_id = r.creator_id
            WHERE p.is_rising
            """
        ).fetchall()
        assert joined == [(1, 3.0, 30.0, 10.0)]


def test_dominant_domain_tie_broken_alphabetically_null_ignored(db):
    """Most frequent enriched domain wins; ties go alphabetical; unenriched
    posts (NULL domain) never count."""
    with db.get_connection() as con:
        posts = [
            # creator 1: 'tech' x2 vs 'art' x2 → tie → 'art'
            _post("d1", 1, domain="tech", topic="x", days_ago=9),
            _post("d2", 1, domain="art", topic="x", days_ago=8),
            _post("d3", 1, domain="tech", topic="x", days_ago=7),
            _post("d4", 1, domain="art", topic="x", days_ago=6),
            _post("d5", 1, domain=None, days_ago=5),  # unenriched
            # creator 2: no enriched posts at all → NULL dominant domain
            _post("d6", 2, domain=None, days_ago=5),
        ]
        _seed(con, posts, [_label(p[0]) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        doms = dict(
            con.execute(
                "SELECT creator_id, dominant_domain FROM v_creator_profile"
                " WHERE creator_id IN (1, 2)"
            ).fetchall()
        )
        assert doms[1] == "art"
        assert doms[2] is None


def test_creator_profile_is_gate_free_with_true_averages(db):
    """Every creator appears with real counts; avg_engagement_score ignores
    unscored posts (mean of non-NULL)."""
    with db.get_connection() as con:
        posts = [
            _post("g1", 1, likes=10, comments=0, days_ago=3, topic="t"),
            _post("g2", 1, likes=20, comments=0, days_ago=2, topic="t"),
            # creator 2: 3 posts, only 2 scored (2 + 6) → avg 4.0
            _post("h1", 2, likes=5, comments=0, days_ago=3, topic="t"),
            _post("h2", 2, likes=7, comments=0, days_ago=2, topic="t"),
            _post("h3", 2, likes=9, comments=0, days_ago=1),
        ]
        labels = [
            _label("g1", likes_z=2.0), _label("g2", likes_z=0.0),
            _label("h1", likes_z=4.0), _label("h2", likes_z=4.0),
            _label("h3", likes_z=None),
        ]
        _seed(con, posts, labels)
    _run_metrics_chain(db)

    with db.get_connection() as con:
        row = con.execute(
            "SELECT total_posts, avg_engagement_score FROM v_creator_profile"
            " WHERE creator_id = 2"
        ).fetchone()
        assert row[0] == 3  # gate-free: 2 scored + 1 unscored post
        assert row[1] == pytest.approx(2.0)  # mean of 0.5·4 and 0.5·4
        # all 2 creators present even though below any enrichment gate
        assert con.execute(
            "SELECT COUNT(*) FROM v_creator_profile"
        ).fetchone()[0] == 2


# ── v_creator_topics: top-5 by count and by performance ─────────────────────


def test_topics_top5_by_count_or_perf_ties_share_rank(db):
    """Rows survive when top-5 by EITHER rank; RANK ties share a rank;
    perf_score is the mean of SCORED member posts only."""
    with db.get_connection() as con:
        posts = []
        counts = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 2, "F": 1, "G": 1}
        for topic, n in counts.items():
            for k in range(n):
                # G: only unscored posts; A: 2 of 5 unscored (mean of 3 kept)
                scored = topic != "G" and (topic != "A" or k < 3)
                posts.append(
                    _post(f"{topic}{k}", 1, topic=topic, days_ago=5 + k,
                          likes=10)
                )
        # perf: F best (10.0), then E 5, D 4, C 3, B 2, A 1, G none
        perf = {"A": 1.0, "B": 2.0, "C": 3.0, "D": 4.0, "E": 5.0, "F": 10.0}
        labels = [
            _label(p[0], likes_z=perf[p[0][0]] * 2 if p[0][0] != "G" else None)
            for p in posts
            if p[0][0] != "A" or int(p[0][1]) < 3
        ]
        # give the unscored A posts NULL likes_z but a label row (score NULL
        # only when ALL z NULL — no comments/views baselines here)
        labels += [_label(f"A{k}", likes_z=None) for k in (3, 4)]
        _seed(con, posts, labels)
    _run_metrics_chain(db)

    with db.get_connection() as con:
        rows = con.execute(
            "SELECT topic, post_count, perf_score, perf_rank, count_rank"
            " FROM v_creator_topics ORDER BY topic"
        ).fetchall()
        by_topic = {r[0]: r[1:] for r in rows}
        # A..F kept (top-5 by count; F additionally top-1 by perf)
        assert set(by_topic) == {"A", "B", "C", "D", "E", "F"}
        # G is 6th by count AND last by perf → dropped
        # count ranks: A=1, B=2, C=3, D=E=4 (tie), F=G=6 (tie) → D,E,F kept
        assert by_topic["D"][3] == 4 and by_topic["E"][3] == 4
        assert by_topic["F"][3] == 6
        # perf ranks: F=1, E=2, D=3, C=4, B=5, A=6 (G NULLS LAST, dropped)
        assert by_topic["F"][2] == 1 and by_topic["A"][2] == 6
        # perf_score = mean of scored member posts: A has 3 scored of 5
        assert by_topic["A"][1] == pytest.approx(1.0)
        assert by_topic["A"][0] == 5  # all 5 enriched posts count


def test_topics_exclude_unenriched_posts(db):
    """Only enriched posts (gold_topic present) form topic rollups."""
    with db.get_connection() as con:
        posts = [
            _post("e1", 1, topic="chess", days_ago=3),
            _post("e2", 1, topic="chess", days_ago=2),
            _post("u1", 1, topic=None, days_ago=2),  # unenriched
        ]
        _seed(con, posts, [_label(p[0], likes_z=1.0) for p in posts])
    _run_metrics_chain(db)

    with db.get_connection() as con:
        rows = con.execute(
            "SELECT topic, post_count, perf_score FROM v_creator_topics"
        ).fetchall()
        assert rows == [("chess", 2, 0.5)]  # 0.5 · 1.0
