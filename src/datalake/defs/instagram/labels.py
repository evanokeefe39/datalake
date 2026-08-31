"""Tukey-fence label pass for Instagram posts (Epic 3, US-L1/L2/L6).

Estimator LOCKED (implementation plan §4): a post is a standout iff

    likes > Q3 + 1.5 * IQR

where Q3/IQR come from the trailing baseline: the N=20 posts published
BEFORE the post (self-excluded) by the same creator, expanded to a 90-day
lookback when fewer than 20 prior posts exist, with a minimum n of 5
(below that the post is ``insufficient_baseline``). ``baseline_center``
records Q3 and ``baseline_spread`` records IQR.

Self-versioning: ``LABEL_VERSION`` is bumped in the SAME commit as any
estimator/rule change. The pass recomputes every row whose
``label_version`` differs; ``day7_matched`` labels are immutable under
re-judgment (they only recompute on a version bump); provisional
``day0_heuristic`` rows upgrade exactly once to ``day7_matched`` when the
post matures.

``bootstrap=True`` (scripts/migrate_backfill_labels.py) forces every row
to ``day0_heuristic``/provisional — the bootstrap is a point-in-time
snapshot, never a day7 judgment.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from datalake.defs.common.schemas import duckdb_ddl

logger = logging.getLogger(__name__)

#: Bump in the SAME commit as any estimator/rule change so stored labels
#: self-report staleness (US-L2). v1 = initial Tukey fence (post-Q3 + 1.5·IQR).
LABEL_VERSION = 1

#: Enrichment-approving decisions (the admission drain set).
APPROVED_DECISIONS = ("standout", "control", "floor_filler")

_BASELINE_N = 20
_BASELINE_MIN_N = 5
_BASELINE_WINDOW_DAYS = 90
_MATURE_DAYS = 7

_DDL = duckdb_ddl("ig_post_labels")


def _control_flags(conn, post_ids: list[str]) -> dict[str, bool]:
    """Deterministic control membership.

    The plan specifies ``abs(hashtext(post_id)) % 100 < 10``, but this DuckDB
    build has no ``hashtext`` scalar — DuckDB's stable ``hash()`` (UBIGINT)
    fills the same role: deterministic per post_id, ~10% membership.
    """
    if not post_ids:
        return {}
    conn.register("control_ids", __import__("polars").DataFrame({"post_id": post_ids}).to_arrow())
    rows = conn.execute(
        "SELECT post_id, hash(post_id) % 100 < 10 FROM control_ids"
    ).fetchall()
    conn.unregister("control_ids")
    return {pid: bool(flag) for pid, flag in rows}


def _load_inputs(conn):
    """Posts, latest non-sentinel likes per post, existing label post_ids."""
    posts = [
        dict(zip(
            ("post_id", "owner_id", "owner_username", "caption", "ts", "processed_on"), r
        ))
        for r in conn.execute(
            "SELECT post_id, owner_id, owner_username, caption, timestamp, processed_on "
            "FROM silver_ig_posts"
        ).fetchall()
    ]
    likes: dict[str, int | None] = {}
    for pid, lc in conn.execute("""
        SELECT post_id, likes_count FROM (
            SELECT post_id, likes_count,
                   row_number() OVER (
                       PARTITION BY post_id
                       ORDER BY (likes_count IS NULL OR likes_count < 0),
                                observed_at DESC
                   ) AS rn
            FROM silver_ig_post_observations
        ) WHERE rn = 1
    """).fetchall():
        likes[pid] = lc
    # Fallback: posts with no observation use current silver likes (the
    # silver value is the deduped newest scrape).
    for pid, lc in conn.execute(
        "SELECT post_id, likes_count FROM silver_ig_posts"
    ).fetchall():
        likes.setdefault(pid, lc)
    existing = {
        r[0]
        for r in conn.execute("SELECT post_id FROM ig_post_labels").fetchall()
    }
    return posts, likes, existing


def _load_existing(conn) -> dict[str, dict]:
    rows = conn.execute(
        "SELECT post_id, method, is_provisional, label_version FROM ig_post_labels"
    ).fetchall()
    return {
        pid: {"method": m, "is_provisional": prov, "label_version": ver}
        for pid, m, prov, ver in rows
    }


def run_label_pass(
    conn,
    core_handles: set[str] | None = None,
    *,
    bootstrap: bool = False,
    now: datetime | None = None,
) -> dict:
    """Judge every silver post and upsert ``ig_post_labels``.

    Returns stats: stamped / by_method / by_decision / upgraded_day7 /
    kept_day7 / candidates_seen. Idempotent and additive-only.
    """
    import polars as pl

    conn.execute(_DDL)
    core_handles = {h.lower().lstrip("@") for h in (core_handles or set())}
    now = now or datetime.now(timezone.utc)
    now = now.replace(tzinfo=None) if now.tzinfo else now  # silver is naive UTC

    posts, likes, _ = _load_inputs(conn)
    existing = _load_existing(conn)
    control = _control_flags(conn, [p["post_id"] for p in posts])

    # ── Group by creator, order by publish time ──────────────────────────
    by_creator: dict[str, list[dict]] = {}
    for p in posts:
        key = (p["owner_id"] or "").lower()
        by_creator.setdefault(key, []).append(p)
    for plist in by_creator.values():
        plist.sort(key=lambda p: p["ts"] or p["processed_on"] or datetime.min.replace(tzinfo=timezone.utc))

    out: dict[str, tuple] = {}  # post_id -> row tuple
    stats = {
        "stamped": 0, "upgraded_day7": 0, "kept_day7": 0,
        "candidates_seen": len(posts), "bootstrap": bootstrap,
    }
    by_method: dict[str, int] = {}
    by_decision: dict[str, int] = {}

    for creator_key, plist in by_creator.items():
        is_core = False
        for p in plist:
            known = likes.get(p["post_id"])
            judged_like = known if (known is not None and known >= 0) else None
            caption = (p["caption"] or "").strip()
            empty_caption = not caption
            is_core = (p["owner_username"] or "").lower().lstrip("@") in core_handles

            # ── Baseline (only for posts that could be judged) ───────────
            baseline_center = baseline_spread = None
            baseline_n = None
            standout = False
            if judged_like is not None:
                pub = p["ts"] or p["processed_on"]
                prior_posts = [
                    q for q in plist
                    if q["post_id"] != p["post_id"]
                    and q["ts"] and pub and q["ts"] < pub
                    and likes.get(q["post_id"]) is not None
                    and likes[q["post_id"]] >= 0
                ]
                # Trailing N=20 prior posts; expand to a 90-day lookback
                # when fewer than 20 prior posts exist.
                if len(prior_posts) < _BASELINE_N and pub:
                    cutoff = pub - timedelta(days=_BASELINE_WINDOW_DAYS)
                    window_posts = [q for q in prior_posts if q["ts"] >= cutoff]
                else:
                    window_posts = prior_posts[-_BASELINE_N:]
                window = [likes[q["post_id"]] for q in window_posts]
                n = len(window)
                baseline_n = n
                if n >= _BASELINE_MIN_N:
                    q1 = _quantile(window, 0.25)
                    q3 = _quantile(window, 0.75)
                    iqr = float(q3 - q1)
                    baseline_center = float(q3)
                    baseline_spread = iqr
                    standout = judged_like > q3 + 1.5 * iqr
            judgeable = judged_like is not None and baseline_n is not None \
                and baseline_n >= _BASELINE_MIN_N

            # ── Rule table (plan §4) ─────────────────────────────────────
            maturity_days = None
            if judged_like is None:
                label, method, decision, prov = "unjudgeable", "day0_heuristic", "control", True
            elif not judgeable:
                label, method, decision, prov = "insufficient_baseline", "day0_heuristic", "control", True
            elif empty_caption:
                label = "standout" if standout else "average" if judgeable else "unjudgeable"
                method, decision, prov = "day0_heuristic", "skip", True
            elif is_core and not bootstrap:
                age_days = (now - (p["ts"] or p["processed_on"] or now)).days
                if age_days < _MATURE_DAYS:
                    label, method, decision, prov = "pending", "pending", "skip", True
                else:
                    maturity_days = age_days
                    label = "standout" if standout else "average"
                    method = "day7_matched"
                    prov = False
                    decision = (
                        "standout" if standout
                        else "control" if control.get(p["post_id"])
                        else "skip"
                    )
            else:
                maturity_days = None
                label = "standout" if standout else "average"
                method = "day0_heuristic"
                prov = True
                decision = (
                    "standout" if standout
                    else "control" if control.get(p["post_id"])
                    else "skip"
                )

            if bootstrap and method == "day7_matched":
                # Bootstrap snapshot: force provisional day0 so the daily
                # pass performs the single day0→day7 upgrade later.
                method, prov = "day0_heuristic", True

            out[p["post_id"]] = (
                p["post_id"], label, method, decision, now, maturity_days, prov,
                LABEL_VERSION, baseline_center, baseline_spread, baseline_n,
            )
        # Floor-filler top-up applies in bootstrap too (decision is day0-compatible).
        _apply_floor_filler(plist, out, likes)

    # ── Merge with existing labels (US-L2 immutability/upgrade rules) ────
    rows_to_write = []
    for pid, row in out.items():
        ex = existing.get(pid)
        if (
            ex is not None
            and ex["method"] == "day7_matched"
            and not ex["is_provisional"]
            and ex["label_version"] == LABEL_VERSION
        ):
            stats["kept_day7"] += 1
            continue  # day7 never overwritten
        if (
            ex is not None
            and ex["method"] == "day0_heuristic"
            and row[2] == "day7_matched"
        ):
            stats["upgraded_day7"] += 1
        rows_to_write.append(row)

    if rows_to_write:
        df = pl.DataFrame(
            rows_to_write,
            schema=[
                "post_id", "label", "method", "enrich_decision", "judged_at",
                "maturity_days", "is_provisional", "label_version",
                "baseline_center", "baseline_spread", "baseline_n",
            ],
            orient="row",
        )
        conn.register("labels_new", df.to_arrow())
        conn.execute(
            "INSERT OR REPLACE INTO ig_post_labels SELECT * FROM labels_new"
        )
        conn.unregister("labels_new")
    stats["stamped"] = len(rows_to_write)
    for _, _, method, decision, _, _, _, _, _, _, _ in rows_to_write:
        by_method[method] = by_method.get(method, 0) + 1
        by_decision[decision] = by_decision.get(decision, 0) + 1
    stats["by_method"] = by_method
    stats["by_decision"] = by_decision
    return stats


def _apply_floor_filler(plist: list[dict], out: dict, likes: dict) -> None:
    """Promote a creator's top non-standout posts to floor_filler until the
    rankability floor (>=3 enrichment-approved posts) clears. Only applies
    when the creator has at least one standout."""
    standout_ids = [
        pid for pid, row in
        ((p["post_id"], out.get(p["post_id"])) for p in plist)
        if row is not None and row[1] == "standout"
    ]
    if not standout_ids:
        return
    approved = [
        p for p in plist
        if (row := out.get(p["post_id"])) is not None
        and row[3] in APPROVED_DECISIONS
    ]
    need = 3 - len(approved)
    if need <= 0:
        return
    candidates = [
        p for p in plist
        if (row := out.get(p["post_id"])) is not None
        and row[3] == "skip"
        and (p["caption"] or "").strip()
        and (likes.get(p["post_id"]) is not None and likes[p["post_id"]] >= 0)
    ]
    candidates.sort(
        key=lambda p: likes.get(p["post_id"]) or 0, reverse=True
    )
    for p in candidates[:need]:
        row = out[p["post_id"]]
        out[p["post_id"]] = row[:3] + ("floor_filler",) + row[4:]


def stale_label_count(conn) -> int:
    """Rows whose label_version != LABEL_VERSION (US-L2 mismatch check)."""
    return conn.execute(
        "SELECT COUNT(*) FROM ig_post_labels WHERE label_version != ?",
        [LABEL_VERSION],
    ).fetchone()[0]


def _quantile(values: list, q: float) -> float:
    """Linear-interpolation quantile (numpy default / quantile_cont)."""
    s = sorted(float(v) for v in values)
    pos = (len(s) - 1) * q
    lo = int(pos)
    if lo + 1 >= len(s):
        return s[lo]
    frac = pos - lo
    return s[lo] + (s[lo + 1] - s[lo]) * frac
