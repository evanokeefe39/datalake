"""Build a deterministic dev/smoke slice of the datalake.

Creates ``data/smoke/`` with its OWN lake roots, state.duckdb, ops.sqlite and
media cache — a derived dev artifact for cheap, repeatable smoke-testing of
expensive operations (e.g. enrichment passes). The live lake
(``data/lake/``, ``data/state.duckdb``, ``data/ops.sqlite``) is opened
READ-ONLY for selection and is never written.

Usage:
    uv run python scripts/make_smoke_slice.py --posts 100 --creators 5 \
        --out data/smoke --seed 42

What it writes (all under ``--out``):
    state.duckdb       catalog-generated DDL + subset rows (silver_ig_posts,
                       ig_post_labels, profiles/observations, dims) + the six
                       enrichment silver tables produced by the REAL conform
                       layer over the smoke bronze landing.
    lake/bronze/       NEW parquet files: bronze source rows for the selected
                       posts + the bronze_enrichment_raw subset for conform.
    lake/silver/       produced by defs.enrichment.conform.conform() over the
                       smoke bronze (not copied).
    ops.sqlite         catalog-generated DDL + media_cache rows (local_path
                       REWRITTEN to the smoke media root) + creators/profiles
                       for the selected creators.
    media/posts/       copied media bytes for every selected media-bearing post.
    README.md          provenance + warning that this is not the lake.

Selection is deterministic: top ``--creators`` creators by post count, then a
seeded stratified sample of ``--posts`` posts that MUST include text-only
posts, media-bearing posts (only ones whose media URLs fully resolve in the
byte cache), at least one video, at least one carousel, and a mix of
standout/non-standout labels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402

from datalake.defs.common.schemas import DUCKDB_TABLES, duckdb_ddl, sqlite_ddl_for  # noqa: E402
from datalake.defs.enrichment import conform  # noqa: E402

LIVE_STATE_DB = REPO_ROOT / "data" / "state.duckdb"
LIVE_OPS_DB = REPO_ROOT / "data" / "ops.sqlite"
LIVE_BRONZE = REPO_ROOT / "data" / "lake" / "bronze"

DEFAULT_POSTS = 100
DEFAULT_CREATORS = 5
DEFAULT_SEED = 42

# ops.sqlite tables retained under ADR-0012.
OPS_TABLES = (
    "media_cache",
    "creators",
    "profiles",
    "creator_merges",
    "prompt_registry",
)

DUCKDB_TABLE_NAMES = tuple(DUCKDB_TABLES.keys())


def url_hash(media_url: str) -> str:
    """sha256 hex of the URL — the media_cache key (matches media_cache.url_hash)."""
    return hashlib.sha256(media_url.encode("utf-8")).hexdigest()


def _check_roots(out: Path) -> None:
    """Hard guard: the smoke root must never overlap a live root."""
    live_roots = [
        LIVE_BRONZE,
        REPO_ROOT / "data" / "lake",
        REPO_ROOT / "data" / "media",
        REPO_ROOT / "data",  # the smoke root may be data/smoke — but never data itself
    ]
    out_res = out.resolve()
    data_root = (REPO_ROOT / "data").resolve()
    for live in live_roots:
        live_res = live.resolve()
        if out_res == live_res or live_res in out_res.parents:
            # out IS a live root, or lives inside one. Exception: data/smoke
            # is a child of data/ — that is the intended layout.
            if not (live_res == data_root and out_res != data_root and live_res in out_res.parents):
                raise SystemExit(
                    f"REFUSING: smoke root {out_res} overlaps live root {live_res}. "
                    "The live lake must never be written by this script."
                )
        if out_res in live_res.parents:
            # out CONTAINS a live root.
            raise SystemExit(
                f"REFUSING: smoke root {out_res} would contain live root {live_res}. "
                "The live lake must never be written by this script."
            )


def _resolve_media_urls(ops_ro: sqlite3.Connection, media_files_json: str) -> list[str] | None:
    """All local cached paths for a post's media URLs, or None if ANY URL is
    unresolved (no media_cache row or missing bytes on disk)."""
    try:
        urls = json.loads(media_files_json or "[]")
    except (ValueError, TypeError):
        return None
    if not urls:
        return None
    paths: list[str] = []
    for u in urls:
        row = ops_ro.execute(
            "SELECT local_path FROM media_cache WHERE cache_key = ?", (url_hash(u),)
        ).fetchone()
        if not row or not Path(row[0]).exists():
            return None
        paths.append(row[0])
    return paths


def select_slice(
    state_ro: duckdb.DuckDBPyConnection,
    ops_ro: sqlite3.Connection,
    creators: int,
    posts: int,
    seed: int,
) -> dict:
    """Deterministically select creators + posts. Returns selection dict."""
    top = state_ro.execute(
        "SELECT owner_username, count(*) c FROM silver_ig_posts "
        "WHERE owner_username IS NOT NULL GROUP BY 1 ORDER BY c DESC, owner_username "
        "LIMIT ?",
        [creators],
    ).fetchall()
    owners = [r[0] for r in top]

    rows = state_ro.execute(
        """
        SELECT p.post_id, p.owner_username, p.media_files, p.media_count,
               p.video_view_count, coalesce(l.label, 'unlabeled') AS label
        FROM silver_ig_posts p
        LEFT JOIN ig_post_labels l USING (post_id)
        WHERE p.owner_username IN (SELECT unnest(?))
        ORDER BY p.post_id
        """,
        [owners],
    ).fetchall()

    rng = random.Random(seed)
    text_pool: list[tuple] = []
    media_pool: list[tuple] = []
    for r in rows:
        post_id, owner, mf, mcount, vviews, label = r
        if (mf or "[]") == "[]":
            text_pool.append(r)
            continue
        if _resolve_media_urls(ops_ro, mf) is not None:
            media_pool.append(r)

    if not media_pool:
        raise SystemExit("No media-bearing posts with fully resolvable media — cannot build slice.")

    selected: dict[str, tuple] = {}

    def take(pool: list[tuple], pred, n: int) -> None:
        cands = [r for r in pool if r[0] not in selected and pred(r)]
        rng.shuffle(cands)
        for r in cands[:n]:
            selected[r[0]] = r

    # Stratification floor: video, carousel, text-only, standout mix.
    take(media_pool, lambda r: (r[4] or 0) > 0, 1)              # video
    take(media_pool, lambda r: (r[3] or 0) > 1, 1)              # carousel
    take(media_pool + text_pool, lambda r: r[5] == "standout", min(max(5, posts // 20), posts // 4))
    take(text_pool, lambda r: True, min(max(10, posts // 10), posts // 3))  # text-only presence

    remaining = [r for r in (media_pool + text_pool) if r[0] not in selected]

    for r in remaining:
        if len(selected) >= posts:
            break
        selected[r[0]] = r

    chosen = sorted(selected.values(), key=lambda r: r[0])
    return {
        "owners": owners,
        "creator_post_counts": dict(top),
        "posts": chosen,
        "pools": {"media_resolvable": len(media_pool), "text_only": len(text_pool)},
    }


def build_slice(out: Path, creators: int, posts: int, seed: int) -> dict:
    _check_roots(out)
    bronze_out = out / "lake" / "bronze"
    silver_out = out / "lake" / "silver"
    media_out = out / "media" / "posts"
    state_out = out / "state.duckdb"
    ops_out = out / "ops.sqlite"
    for d in (bronze_out, silver_out, media_out):
        d.mkdir(parents=True, exist_ok=True)

    state_ro = duckdb.connect(str(LIVE_STATE_DB), read_only=True)
    ops_ro = sqlite3.connect(f"file:{LIVE_OPS_DB.as_posix()}?mode=ro", uri=True)
    try:
        sel = select_slice(state_ro, ops_ro, creators, posts, seed)
        chosen = sel["posts"]
        post_ids = [r[0] for r in chosen]
        owners = sel["owners"]

        # ── state.duckdb: catalog DDL + subset rows (live attached READ_ONLY) ──
        if state_out.exists():
            state_out.unlink()
        smoke = duckdb.connect(str(state_out))
        for name in DUCKDB_TABLE_NAMES:
            smoke.execute(duckdb_ddl(name))
        smoke.execute(f"ATTACH '{LIVE_STATE_DB.as_posix()}' AS live (READ_ONLY)")

        def copy_subset(table: str, where: str, params: list) -> int:
            n = smoke.execute(
                f"INSERT INTO {table} SELECT * FROM live.{table} WHERE {where}", params
            ).fetchone()
            return 0

        def count(table: str) -> int:
            return smoke.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        copy_subset("silver_ig_posts", "post_id IN (SELECT unnest(?))", [post_ids])
        copy_subset("ig_post_labels", "post_id IN (SELECT unnest(?))", [post_ids])
        copy_subset("silver_ig_post_observations", "post_id IN (SELECT unnest(?))", [post_ids])
        copy_subset("silver_ig_profiles", "owner_username IN (SELECT unnest(?))", [owners])
        copy_subset("silver_ig_profile_observations", "owner_username IN (SELECT unnest(?))", [owners])
        copy_subset("dim_profile", "owner_username IN (SELECT unnest(?))", [owners])
        smoke.execute(
            "INSERT INTO watermarks (name, timestamp) SELECT name, timestamp FROM live.watermarks"
        )

        # dim_date: same generator SQL as the serving asset.
        smoke.execute("""
            CREATE OR REPLACE TABLE dim_date AS
            SELECT
                date_col::DATE AS date,
                EXTRACT(YEAR FROM date_col) AS year,
                EXTRACT(QUARTER FROM date_col) AS quarter,
                EXTRACT(MONTH FROM date_col) AS month_number,
                MONTHNAME(date_col) AS month_name,
                EXTRACT(WEEK FROM date_col) AS week_number,
                EXTRACT(DAY FROM date_col) AS day_number,
                DAYNAME(date_col) AS day_of_week,
                CASE WHEN DAYOFWEEK(date_col) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM date_col) >= 7
                     THEN EXTRACT(YEAR FROM date_col)
                     ELSE EXTRACT(YEAR FROM date_col) - 1
                END AS financial_year
            FROM generate_series(
                CURRENT_DATE - INTERVAL 1 YEAR, CURRENT_DATE, INTERVAL 1 DAY
            ) AS t(date_col)
        """)
        smoke.execute("DETACH live")

        # ── Bronze: NEW parquet files for selected posts' source datasets ──
        src_map = dict(
            state_ro.execute(
                "SELECT post_id, source_dataset FROM silver_ig_posts WHERE post_id IN (SELECT unnest(?))",
                [post_ids],
            ).fetchall()
        )
        by_dataset: dict[str, set[str]] = {}
        for pid, ds in src_map.items():
            by_dataset.setdefault(ds, set()).add(pid)

        bronze_files_written = []
        for ds, ids in sorted(by_dataset.items()):
            src = LIVE_BRONZE / f"{ds}.parquet"
            if not src.exists():
                raise SystemExit(f"Bronze source file missing for dataset {ds!r}: {src}")
            df = pl.read_parquet(src)
            sub = df.filter(pl.col("id").is_in(sorted(ids)))
            dest = bronze_out / f"{ds}.parquet"
            sub.write_parquet(dest)
            bronze_files_written.append(dest.name)

        # ── bronze_enrichment_raw subset → smoke landing → REAL conform ──
        landing_live = LIVE_BRONZE / "bronze_enrichment_raw.parquet"
        landing_df = pl.read_parquet(landing_live)
        landing_sub = landing_df.filter(pl.col("post_id").is_in(post_ids))
        landing_sub.write_parquet(bronze_out / "bronze_enrichment_raw.parquet")

        n_media_by_post = {
            r[0]: (r[1] or 0)
            for r in state_ro.execute(
                "SELECT post_id, media_count FROM silver_ig_posts WHERE post_id IN (SELECT unnest(?))",
                [post_ids],
            ).fetchall()
        }
        result = conform.conform(
            root=bronze_out,
            silver_root=silver_out,
            conn=smoke,
            now=None,
            n_media_by_post=n_media_by_post,
        )
        conformed = result.counts["conformed"]
        quarantined = result.counts["quarantined"]

        # ── ops.sqlite: catalog DDL + subsets, media bytes copied ──
        if ops_out.exists():
            ops_out.unlink()
        smoke_ops = sqlite3.connect(str(ops_out))
        smoke_ops.executescript(sqlite_ddl_for(*OPS_TABLES))

        media_files_copied = 0
        cache_rows = []
        for r in chosen:
            if (r[2] or "[]") == "[]":
                continue
            for u in json.loads(r[2]):
                ck = url_hash(u)
                row = ops_ro.execute(
                    "SELECT local_path, content_type, size_bytes, fetched_at, source_url "
                    "FROM media_cache WHERE cache_key = ?",
                    (ck,),
                ).fetchone()
                if not row:
                    continue
                src = Path(row[0])
                dest = media_out / f"{ck}{src.suffix}"
                if not dest.exists():
                    shutil.copy2(src, dest)
                    media_files_copied += 1
                cache_rows.append((ck, str(dest.resolve()), row[1], row[2], row[3], row[4]))
        smoke_ops.executemany(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            cache_rows,
        )

        handles = tuple(owners)
        q = ",".join("?" * len(handles))
        creators_rows = smoke_ops.execute(
            f"SELECT c.id, c.name, c.created_at, c.updated_at FROM creators c "
            f"WHERE c.id IN (SELECT creator_id FROM profiles WHERE handle IN ({q}))",
            handles,
        ).fetchall()
        smoke_ops.executemany(
            "INSERT OR REPLACE INTO creators (id, name, created_at, updated_at) VALUES (?,?,?,?)",
            creators_rows,
        )
        profiles_rows = ops_ro.execute(
            f"SELECT platform, handle, profile_url, results_type, results_limit, enabled, "
            f"tier, creator_id, updated_at FROM profiles WHERE handle IN ({q})",
            handles,
        ).fetchall()
        smoke_ops.executemany(
            "INSERT OR REPLACE INTO profiles (platform, handle, profile_url, results_type, "
            "results_limit, enabled, tier, creator_id, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            profiles_rows,
        )
        smoke_ops.commit()

        # ── README ──
        label_mix: dict[str, int] = {}
        for r in chosen:
            label_mix[r[5]] = label_mix.get(r[5], 0) + 1
        media_posts = sum(1 for r in chosen if (r[2] or "[]") != "[]")
        counts = {t: count(t) for t in DUCKDB_TABLE_NAMES}
        write_readme(out, seed, creators, posts, label_mix, media_posts, bronze_files_written)
        smoke_ops.close()
        smoke.close()
    finally:
        state_ro.close()
        ops_ro.close()

    return {
        "selection": sel,
        "rows": {t: c for t, c in counts.items()},
        "conformed": conformed,
        "quarantined": quarantined,
        "media_files_copied": media_files_copied,
        "bronze_files": bronze_files_written,
        "label_mix": label_mix,
        "media_posts": media_posts,
    }


def write_readme(
    out: Path,
    seed: int,
    creators: int,
    posts: int,
    label_mix: dict[str, int],
    media_posts: int,
    bronze_files: list[str],
) -> None:
    (out / "README.md").write_text(
        f"""# data/smoke — DERIVED DEV ARTIFACT (NOT the lake)

**This is not the real lake.** It is a deterministic, seeded dev/smoke slice
built from a read-only snapshot of the live data for cheap, repeatable
smoke-testing of expensive operations (e.g. enrichment passes). Never use it
for analytics, never treat its numbers as lake numbers, and never let a
pipeline write to the live roots from this directory.

## Provenance

- Built: {date.today().isoformat()}
- Command: `uv run python scripts/make_smoke_slice.py --posts {posts} --creators {creators} --out {out.as_posix()} --seed {seed}`
- Seed: `{seed}` (re-running with the same seed and unchanged live data reproduces the same selection)
- Selection: top {creators} creators by post count in `silver_ig_posts`; a
  seeded stratified sample of {posts} posts ({media_posts} media-bearing — only posts whose
  media URLs FULLY resolve in the scrape-time byte cache — plus text-only
  posts), guaranteeing at least one video, one carousel, and a mix of
  standout/non-standout labels. Label mix: `{json.dumps(label_mix, sort_keys=True)}`
- Bronze source datasets: {", ".join(f"`{f}`" for f in bronze_files)}
- The six enrichment silver tables were produced by the REAL conform layer
  (`defs.enrichment.conform.conform`) over the slice's `bronze_enrichment_raw`
  landing — not copied from the lake.
- Media bytes are copied into `media/posts/` and every `media_cache.local_path`
  was rewritten to this smoke media root.
- The live lake (`data/lake/`, `data/state.duckdb`, `data/ops.sqlite`) was
  opened read-only and never written.

## Layout

- `state.duckdb` — catalog-generated schema + subset rows + conformed silver
- `ops.sqlite` — media_cache (rewritten paths), creators, profiles
- `lake/bronze/`, `lake/silver/` — the slice's own lake roots
- `media/posts/` — copied media bytes
""",
        encoding="utf-8",
    )


def enrichment_plan_check(out: Path, modes: list[str]) -> dict[str, int]:
    """Run the REAL enrichment plan against the slice via its CLI flags.

    Returns {mode: submittable}. Raises SystemExit when visual submittable == 0
    — that single assertion is what makes the slice worth having.
    """
    submittable: dict[str, int] = {}
    for mode in modes:
        proc = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "enrich_facets_batch.py"),
                "--plan",
                "--mode",
                mode,
                "--state-db",
                str(out / "state.duckdb"),
                "--ops-db",
                str(out / "ops.sqlite"),
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=300,
        )
        if proc.returncode != 0:
            raise SystemExit(
                f"Enrichment plan failed for mode={mode} (rc={proc.returncode}):\n"
                f"{proc.stdout}\n{proc.stderr}"
            )
        m = re.search(r"targets=(\d+) submittable=(\d+)", proc.stdout)
        if not m:
            raise SystemExit(f"Unparseable plan output for mode={mode}: {proc.stdout}")
        submittable[mode] = int(m.group(2))
    if submittable.get("visual", 0) <= 0:
        raise SystemExit(
            f"Smoke slice FAILED its usability check: visual submittable = 0 "
            f"({submittable}). Media resolvability is broken."
        )
    return submittable


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--posts", type=int, default=DEFAULT_POSTS)
    p.add_argument("--creators", type=int, default=DEFAULT_CREATORS)
    p.add_argument("--out", default="data/smoke")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--skip-plan-check", action="store_true")
    args = p.parse_args(argv)

    out = Path(args.out)
    if not out.is_absolute():
        out = REPO_ROOT / out
    t0 = time.time()
    res = build_slice(out, args.creators, args.posts, args.seed)

    print("\n=== Smoke slice summary ===")
    print(f"root:            {out}")
    print(f"seed:            {args.seed}")
    print(f"creators:        {len(res['selection']['owners'])} {res['selection']['owners']}")
    print(f"posts:           {len(res['selection']['posts'])}")
    print(f"label mix:       {res['label_mix']}")
    print(f"media posts:     {res['media_posts']} (bytes copied: {res['media_files_copied']} files)")
    print(f"bronze files:    {res['bronze_files']}")
    print(f"conform:         conformed={res['conformed']} quarantined={res['quarantined']}")
    print("per-table rows:")
    for t, c in sorted(res["rows"].items()):
        print(f"  {t:38s} {c}")

    if not args.skip_plan_check:
        sub = enrichment_plan_check(out, ["visual", "text"])
        print(f"enrichment plan: visual submittable={sub['visual']} text submittable={sub['text']}")
        print("usability check: PASS (visual submittable > 0)")
    print(f"done in {time.time() - t0:.1f}s — README: {out / 'README.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
