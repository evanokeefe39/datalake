"""Ad-hoc Instagram account discovery + classification (issue #22).

Recursively expands from seed accounts (in a target niche) via Instagram's
related-accounts rail, enriches each new handle with a public no-login profile
scrape (followers/bio), classifies it into a profile-type taxonomy, and reports
candidates NOT already tracked — so we can grow the full niche population with
depth, not just find the first few seeds.

BAN-FREE BY DESIGN: all scraping runs on Apify's own infra against public
profiles. It NEVER touches the user's Instagram session/cookies.

Usage:
  uv run python scripts/discover_accounts.py \
      --niche "data engineering" --seeds edward.builds,fez.infocus \
      --budget-usd 1.0 --max-new 25

Notes:
- Apify actor runs are invoked with the actor INPUT as the RAW request body
  (not wrapped in {"input": ...}) — verified required for these two actors.
- This is an ad-hoc, budget-tracked script, NOT yet a Dagster asset.
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
    """Related-rail expansion: one actor run per seed (bounded, budget-aware)."""
    found: list[str] = []
    for seed in seeds:
        if budget.over:
            print(f"  [stop] budget reached: {budget}")
            break
        rid = _start_run(tok, ACTOR_RELATED, {"username": [seed], "resultsLimit": per_seed})
        if not rid:
            continue
        items = _await_run(tok, rid)
        cost = budget.record(rid, tok)
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
    # Coarse success proxy absent gold: treat >50 followers as "established";
    # true success (engagement) needs ingestion+gold later.
    return {"size_tier": tier, "niche": niche, "profile_type": f"{tier}_creator", "followers": followers}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--niche", default=DEFAULT_NICHE, help="label for grouping output")
    ap.add_argument("--seeds", required=True, help="comma-separated seed handles")
    ap.add_argument("--per-seed", type=int, default=20, help="related results per seed")
    ap.add_argument("--max-new", type=int, default=25, help="report at most N new accounts")
    ap.add_argument("--max-followers", type=int, default=None,
                    help="only keep/report candidates at or below this follower count (aim small)")
    ap.add_argument("--budget-usd", type=float, default=1.0, help="hard-ish spend cap")
    ap.add_argument("--out", default=None, help="markdown report path (default analysis/output/discovery_<ts>.md)")
    args = ap.parse_args()

    tok = _load_token()
    budget = Budget(args.budget_usd)
    seeds = [s.strip().lower() for s in args.seeds.split(",") if s.strip()]
    tracked = _tracked_handles()
    print(f"niche='{args.niche}' seeds={seeds} tracked={len(tracked)} {budget}")

    # 1) recursive expansion (depth 1; depth>1 is a later refinement)
    candidates = _expand_related(tok, budget, seeds, args.per_seed)
    # drop seeds / already-tracked / dupes
    seen = set(seeds) | tracked
    new = [h for h in candidates if h not in seen]
    new = list(dict.fromkeys(new))  # unique, order-preserving

    # 2) enrich new handles (only what budget allows)
    profiles = _enrich(tok, budget, new) if new else {}

    # 3) classify
    rows = []
    for h in new:
        prof = profiles.get(h, {})
        rows.append({"handle": h, **prof, **(_classify(prof) if prof else {"size_tier": "unenriched",
                     "niche": "unknown", "profile_type": "unenriched", "followers": None})})

    # 4) report
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out = args.out or f"analysis/output/discovery_{ts}.md"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    # Optional aim: drop candidates above a follower ceiling (hunt small).
    if args.max_followers is not None:
        rows = [r for r in rows if (r.get("followers") or 0) <= args.max_followers]
    # Small-first: ascending followers (unenriched last). Truncation under
    # --max-new therefore keeps the LOW tiers (the cohort of interest) and never
    # drops them for the largest — prior DESC behavior discarded smalls.
    ordered = sorted(rows, key=lambda r: (r.get("followers") is None, r.get("followers") or 0))
    shown = ordered[: args.max_new]
    # Full tier distribution across ALL candidates found (not just shown rows).
    import collections
    dist = collections.Counter(_size_tier(r.get("followers")) if r.get("followers") is not None
                               else "unenriched" for r in rows)
    dist_str = ", ".join(f"{k}={v}" for k, v in sorted(dist.items()))
    lines = [f"# Account discovery — {args.niche} ({ts})",
             "", f"Budget: {budget}. Seeds: {', '.join(seeds)}. ",
             f"Candidates: {len(candidates)} unique-new: {len(rows)} "
             f"(tiers: {dist_str}). Tracked roster: {len(tracked)}.",
             "", "| handle | followers | size_tier | niche_guess | profile_type |",
             "|---|---|---|---|---|"]
    for r in shown:
        lines.append(f"| {r['handle']} | {r.get('followers') or ''} | {r['size_tier']} | "
                     f"{r['niche']} | {r['profile_type']} |")
    lines.append("")
    lines.append("> Smallest (low-tier) candidates are listed first and are never truncated "
                 "away. Success signal is NOT yet scored (needs ingestion + gold); "
                 "profile_type is size-tier based.")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"\nWrote {len(rows)} candidate accounts -> {out} (shown {len(shown)})")
    print(f"Session {budget}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
