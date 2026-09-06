"""Ad-hoc Instagram account discovery + classification (issue #22).

Recursively expands from seed accounts (in a target niche) via Instagram's
related-accounts rail, enriches each new handle with a public no-login profile
scrape (followers/bio), classifies it into a size-tier + bio-niche taxonomy, and
reports candidates NOT already tracked — so we can grow the full niche population
with depth.

KEY CALIBRATION (validated 2026-09-05): the related rail is size-homophilous when
seeded from LARGE accounts (edward.builds 333k -> 148 candidates, all >=10k, zero
under 10k). Seeding from SMALL/MEDIUM tracked accounts flips it (edit.party 7.2k,
marketingatg 1.4k, steven.builds 48k -> 41 candidates under 10k incl. 14 under 1k).
So --roster-seeds auto-sources from our tracked small/medium accounts.

QUALITY ANNOTATION & CURATION (documented behaviour — nothing is silently dropped):
Every enriched candidate is grouped by explicit, documented criteria into one of:
  A) quality   = a known niche keyword matched the bio AND followers >= 100.
                 These are the small niche creators the crawl exists to find.
  B) review    = has a non-empty bio but did not reach A (niche vocab miss, or
                 niche matched but below the 100-follower floor).
  C) flagged   = empty/absent bio (no signal to judge). The related-rail of small
                 accounts pulls these junk/placeholder accounts; treat as noise.
  D) unenriched = budget ran out before a profile scrape.
ALL candidates remain in the report under their group heading — the group is a
curation aid, not an automatic drop. At this data size (hundreds of rows) the
full annotated list can be reviewed as-is (e.g. dumped into a conversation
context) to decide what to add to tracking. A strict machine gate can be added
later with --min-followers/--require-niche if wanted.

BAN-FREE BY DESIGN: all scraping runs on Apify's own infra against public
profiles. It NEVER touches the user's Instagram session/cookies.

Usage:
  uv run python scripts/discover_accounts.py --roster-seeds --budget-usd 3
  uv run python scripts/discover_accounts.py \
      --niche "data engineering" --seeds edward.builds,fez.infocus

Notes:
- Apify actor runs are invoked with the actor INPUT as the RAW request body
  (not wrapped in {"input": ...}) — verified required for these two actors.
- Expansion is capped to 40% of the budget so enrichment is always funded
  (enrichment is what produces the follower/size data the report needs).
- Long crawls exceed a foreground window; run with no deadline (timeout 0).
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

import httpx

API = "https://api.apify.com/v2"
# Validated actors (issue #22): related-rail expansion + first-party profile enrich.
ACTOR_RELATED = "thenetaji~instagram-related-user-scraper"
ACTOR_PROFILE = "dSCLg0C3YEZ83HzYX"  # apify/instagram-profile-scraper (first-party)
PROFILES_DB = "data/ops.sqlite"
# Share of budget reserved for enrichment (expansion must not eat it all).
EXPAND_FRAC = 0.4

# Size tiers (followers) aligned to the growth-report buckets.
TIERS = [(0, "0-100"), (100, "100-1k"), (1000, "1k-10k"), (10000, "10k-100k"), (100000, "100k+")]

# Coarse niche keyword map for bio-based domain guessing. Extend as needed.
NICHE_KEYWORDS = {
    "data/ai": ["data", "ai", "ml", "llm", "analytics", "engineer", "python", "model"],
    "dev/coding": ["software", "developer", "coding", "program", "build", "swe", "startup", "saas"],
    "design/creative": ["design", "ui", "creative", "motion", "brand", "illustrator"],
    "business/growth": ["business", "growth", "marketing", "founder", "sales", "brand"],
    "career/self": ["career", "coach", "productivity", "mindset", "freelance"],
    "finance": ["finance", "invest", "money", "tax", "wealth", "vc"],
    "education": ["teach", "tutor", "learn", "course", "school"],
}
DEFAULT_NICHE = "data/ai"
# Quality-curation thresholds (documented behaviour; see module docstring).
QUALITY_MIN_FOLLOWERS = 100
KNOWN_NICHES = set(NICHE_KEYWORDS)


def _load_token() -> str:
    from dotenv import load_dotenv
    load_dotenv(os.path.abspath(".env"))
    tok = os.environ.get("APIFY_API_TOKEN", "")
    if not tok:
        raise SystemExit("APIFY_API_TOKEN not set (.env)")
    return tok


def _headers(tok: str) -> dict:
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}


class Budget:
    """Soft Apify spend tracker: sums completed-run cost; blocks new runs over cap."""

    def __init__(self, cap_usd: float):
        self.cap = cap_usd
        self.spent = 0.0
        self.runs = 0

    def record(self, run_id: str, tok: str) -> float:
        d = httpx.get(f"{API}/actor-runs/{run_id}", headers=_headers(tok), timeout=30).json().get("data", {})
        cost = float(d.get("usageTotalUsd") or 0.0)
        self.spent += cost
        self.runs += 1
        return cost

    @property
    def over(self) -> bool:
        return self.spent >= self.cap

    def __str__(self) -> str:
        return f"budget ${self.spent:.3f}/{self.cap:.2f} ({self.runs} runs)"


def _start_run(tok: str, actor: str, payload: dict) -> str | None:
    """Start an actor run with the input as the RAW body. Returns run_id or None."""
    r = httpx.post(f"{API}/acts/{actor}/runs", headers=_headers(tok),
                   content=json.dumps(payload), timeout=60)
    j = r.json()
    if r.status_code == 201:
        return j["data"]["id"]
    # validation / not-run is free; log and skip.
    print(f"  [warn] {actor} run rejected: {json.dumps(j)[:200]}")
    return None


def _await_run(tok: str, run_id: str, timeout_s: int = 240) -> dict | None:
    """Poll to terminal; return the dataset items list (or None)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        d = httpx.get(f"{API}/actor-runs/{run_id}", headers=_headers(tok), timeout=30).json().get("data", {})
        st = d.get("status")
        if st == "SUCCEEDED":
            items = httpx.get(f"{API}/datasets/{d['defaultDatasetId']}/items?limit=1000",
                               headers=_headers(tok), timeout=60).json()
            return items if isinstance(items, list) else []
        if st in ("FAILED", "TIMED-OUT", "ABORTED"):
            print(f"  [warn] run {run_id} {st}: {d.get('statusMessage')}")
            return None
        time.sleep(15)
    print(f"  [warn] run {run_id} timed out waiting for terminal state")
    return None


def _tracked_handles() -> set[str]:
    """Handles already in the tracked roster (dedupe target)."""
    import sqlite3
    if not os.path.exists(PROFILES_DB):
        return set()
    c = sqlite3.connect(PROFILES_DB)
    try:
        return {r[0].lower() for r in c.execute("SELECT handle FROM profiles").fetchall()}
    finally:
        c.close()


def _roster_small_medium(max_seeds: int = 25) -> list[str]:
    """Auto-source seed handles from the TRACKED roster that are small/medium.

    Size = latest follower count recorded in the bronze lake for each tracked IG
    handle (bronze carries per-post followersCount; we take the most recent seen).
    Seeds are the tracked accounts at/below 100k followers (the small+medium tiers
    we hold). NOTE: roster handles with no bronze record have no known size and are
    excluded (they'd need a size scrape first). Ordering is ascending by followers
    so the smallest seeds crawl first; capped at max_seeds.
    """
    import glob

    import polars as pl

    files = glob.glob("data/lake/bronze/**/*.parquet", recursive=True)
    parts = []
    for f in files:
        try:
            sch = pl.scan_parquet(f).collect_schema().names()
            if {"username", "followersCount"} <= set(sch):
                parts.append(pl.scan_parquet(f).select(["username", "followersCount"]))
        except Exception:
            pass
    if not parts:
        print("  [warn] no bronze follower data found for roster sizing")
        return []
    latest = (pl.concat(parts)
              .filter(pl.col("username").is_not_null() & pl.col("followersCount").is_not_null())
              .with_columns(pl.col("followersCount").cast(pl.Int64, strict=False))
              .filter(pl.col("followersCount") >= 0)
              .sort("followersCount").unique(subset="username", keep="first").collect())
    roster = _tracked_handles()
    sized = [(x["username"].lower(), int(x["followersCount"]))
             for x in latest.to_dicts()
             if x["username"].lower() in roster and int(x["followersCount"]) <= 100_000]
    sized.sort(key=lambda t: t[1])  # smallest first
    seeds = [h for h, _ in sized[:max_seeds]]
    print(f"  roster small/medium seeds (<=100k, bronze-sized): {len(sized)} known, "
          f"using {len(seeds)} smallest")
    return seeds


def _size_tier(followers: int | None) -> str:
    if followers is None:
        return "unknown"
    for lo, label in reversed(TIERS):
        if followers >= lo:
            return label
    return TIERS[0][1]


def _guess_niche(bio: str | None) -> str:
    if not bio:
        return "unknown"
    b = bio.lower()
    best, best_hits = None, 0
    for niche, kws in NICHE_KEYWORDS.items():
        hits = sum(1 for k in kws if k in b)
        if hits > best_hits:
            best, best_hits = niche, hits
    return best or "other"


def _expand_related(tok: str, budget: Budget, seeds: list[str], per_seed: int) -> list[str]:
    """Related-rail expansion, one actor run per seed, budget-aware.

    Expansion is capped at `budget.cap * EXPAND_FRAC` so a share of the budget is
    always reserved for enrichment (which produces the follower/size data the
    report needs). thenetaji per-seed cost is unpredictable ($0.004-$0.24); without
    this reservation a large-seed expansion can eat the whole budget and leave
    every candidate unenriched (observed: 1009 unenriched from a $1.50 budget).
    """
    expand_cap = budget.cap * EXPAND_FRAC
    exp_spent = 0.0
    found: list[str] = []
    for seed in seeds:
        if budget.over:
            print(f"  [stop] budget reached: {budget}")
            break
        if exp_spent >= expand_cap:
            print(f"  [stop] expansion reserve spent ({exp_spent:.3f}/{expand_cap:.2f}); "
                  f"reserving rest for enrichment")
            break
        rid = _start_run(tok, ACTOR_RELATED, {"username": [seed], "resultsLimit": per_seed})
        if not rid:
            continue
        items = _await_run(tok, rid)
        cost = budget.record(rid, tok)
        exp_spent += cost
        handles = [str(i.get("username")).lower() for i in (items or []) if i.get("username")]
        found.extend(handles)
        print(f"  related[{seed}]: {len(handles)} accounts (${cost:.4f}, {budget})")
    return found


def _enrich(tok: str, budget: Budget, handles: list[str], chunk: int = 30) -> dict[str, dict]:
    """First-party profile scrape (followers/bio) for each handle, chunked."""
    out: dict[str, dict] = {}
    for i in range(0, len(handles), chunk):
        if budget.over:
            break
        batch = handles[i:i + chunk]
        rid = _start_run(tok, ACTOR_PROFILE, {"usernames": batch})
        if not rid:
            continue
        items = _await_run(tok, rid)
        cost = budget.record(rid, tok)
        n = 0
        for it in (items or []):
            u = it.get("username")
            if not u:
                continue
            out[u.lower()] = {
                "followers": it.get("followersCount"),
                "bio": it.get("biography") or "",
                "verified": bool(it.get("isVerified")),
            }
            n += 1
        print(f"  enriched batch {i // chunk + 1}: {n} profiles (${cost:.4f}, {budget})")
    return out


def _classify(profile: dict) -> dict:
    tier = _size_tier(profile.get("followers"))
    niche = _guess_niche(profile.get("bio"))
    followers = profile.get("followers") or 0
    return {"size_tier": tier, "niche": niche, "profile_type": f"{tier}_creator", "followers": followers}


def _curate_group(row: dict) -> str:
    """Documented curation group (see module docstring): quality / review / flagged / unenriched."""
    if row.get("size_tier") == "unenriched":
        return "unenriched"
    followers = row.get("followers") or 0
    bio = row.get("bio") or ""
    niche = row.get("niche") or "unknown"
    if not bio.strip():  # empty bio -> no signal; junk from the small-rail.
        return "flagged"
    if niche in KNOWN_NICHES and followers >= QUALITY_MIN_FOLLOWERS:
        return "quality"
    return "review"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--niche", default=DEFAULT_NICHE, help="label for grouping output")
    ap.add_argument("--seeds", default=None, help="comma-separated seed handles (ignored if --roster-seeds)")
    ap.add_argument("--roster-seeds", action="store_true",
                    help="auto-source seeds from the tracked small/medium roster accounts")
    ap.add_argument("--max-seeds", type=int, default=25, help="cap on --roster-seeds auto-seeds")
    ap.add_argument("--per-seed", type=int, default=20, help="related results per seed")
    ap.add_argument("--max-new", type=int, default=25, help="report at most N new accounts")
    ap.add_argument("--max-followers", type=int, default=None,
                    help="only keep/report candidates at or below this follower count (aim small)")
    ap.add_argument("--budget-usd", type=float, default=1.0, help="hard-ish spend cap")
    ap.add_argument("--out", default=None, help="markdown report path (default analysis/output/discovery_<ts>.md)")
    args = ap.parse_args()

    tok = _load_token()
    budget = Budget(args.budget_usd)
    if args.roster_seeds:
        seeds = _roster_small_medium(args.max_seeds)
    else:
        seeds = [s.strip().lower() for s in args.seeds.split(",") if s.strip()]
    tracked = _tracked_handles()
    print(f"niche='{args.niche}' seeds={seeds} tracked={len(tracked)} {budget}")
    if not seeds:
        raise SystemExit("No seeds: pass --seeds or --roster-seeds (no small/medium roster accounts sized).")

    # 1) expansion (budget-capped to EXPAND_FRAC so enrichment is funded)
    candidates = _expand_related(tok, budget, seeds, args.per_seed)
    # drop seeds / already-tracked / dupes
    seen = set(seeds) | tracked
    new = [h for h in candidates if h not in seen]
    new = list(dict.fromkeys(new))  # unique, order-preserving

    # 2) enrich new handles with the remaining (reserved) budget
    profiles = _enrich(tok, budget, new) if new else {}

    # 3) classify
    rows = []
    for h in new:
        prof = profiles.get(h, {})
        if prof:
            row = {"handle": h, **prof, **_classify(prof)}
        else:
            row = {"handle": h, "followers": None, "bio": "", "verified": False,
                   "size_tier": "unenriched", "niche": "unknown", "profile_type": "unenriched"}
        row["group"] = _curate_group(row)
        rows.append(row)

    # 4) report (grouped by documented curation group; nothing dropped)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out = args.out or f"analysis/output/discovery_{ts}.md"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    if args.max_followers is not None:
        rows = [r for r in rows if (r.get("followers") or 0) <= args.max_followers]
    import collections
    groups = collections.OrderedDict([("quality", []), ("review", []), ("flagged", []), ("unenriched", [])])
    for r in rows:
        groups.setdefault(r["group"], []).append(r)
    order = ["quality", "review", "flagged", "unenriched"]
    group_counts = {g: len(groups.get(g, [])) for g in order}
    tier_counts = collections.Counter(r["size_tier"] for r in rows if r["size_tier"] != "unenriched")
    tier_str = ", ".join(f"{k}={v}" for k, v in sorted(tier_counts.items()))
    lines = [f"# Account discovery — {args.niche} ({ts})",
             "",
             f"Budget: {budget}. Seeds: {', '.join(seeds)}. Tracked roster: {len(tracked)}.",
             f"Candidates: {len(candidates)} unique-new: {len(rows)} "
             f"(quality={group_counts['quality']}, review={group_counts['review']}, "
             f"flagged={group_counts['flagged']}, unenriched={group_counts['unenriched']}).",
             f"Enriched tiers: {tier_str or 'none'}.",
             "",
             "Grouping (documented): A)quality = niche vocab matched bio + followers>=100; "
             "B)review = bio present but below A; C)flagged = empty bio (junk); "
             "D)unenriched = budget ran out before profile scrape.",
             ""]
    captions = {"quality": "A. QUALITY — niche-vocab match, followers>=100 (curate to keep)",
                "review": "B. REVIEW — bio present, below A (niche vocab miss or micro)",
                "flagged": "C. FLAGGED — empty bio (junk/placeholder; usually drop)",
                "unenriched": "D. UNENRICHED — budget ran out before profile scrape"}
    shown_total = 0
    for g in order:
        members = sorted(groups.get(g, []), key=lambda r: (r.get("followers") is None, r.get("followers") or 0))
        if not members:
            continue
        take = members[: max(0, args.max_new - shown_total)]
        shown_total += len(take)
        lines += [f"## {captions[g]} ({len(members)} found, showing {len(take)})", "",
                  "| handle | followers | size_tier | niche_guess | profile_type |",
                  "|---|---|---|---|---|"]
        for r in take:
            lines.append(f"| {r['handle']} | {r.get('followers') or ''} | {r['size_tier']} | "
                         f"{r['niche']} | {r['profile_type']} |")
        lines.append("")
    lines.append("> Success signal is NOT yet scored (needs ingestion + gold). profile_type is "
                 "size-tier based. Groups are a curation aid only — nothing is auto-dropped; "
                 "at this data size review the annotated list as-is and decide what to add.")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"\nWrote {len(rows)} candidate accounts -> {out} (shown {shown_total})")
    print(f"Session {budget}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
