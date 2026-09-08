# Epic E-IDENTITY — Creators, profiles & identity consolidation

- **Theme:** Identity (multi-platform enabler)
- **Owner:** dlc-worker (data) / sdlc-worker (UI)
- **Status:** Active
- **Depends on:** E-INGEST
- **Feeds:** E-SERVING-ANALYTICS (dim_profile creator linkage)

## Outcome
A creator (person/brand) owns 1..N profiles (one per platform). Profiles carry
per-platform scrape config; `dim_profile` carries `creator_id`/`creator_name`
for click-through without cross-DB joins. Duplicate auto-creators consolidate
into curated identities with a merge ledger.

## Scope highlights
- `creators` + `profiles` split (replaces `scrape_targets`); depth per-profile.
- `creator_merges` ledger with `--undo`; curated consolidation (21/147/243/610
  retirements).
- Profile management CRUD (add single/batch/edit/remove) + creators UI (list +
  single + click-through everywhere) + avatar download.
- Multi-platform ready (Instagram today; TikTok/YouTube additive).

## Source of truth
`tasks/plans/creators-and-profiles.md`, `profile-management.md`,
`creators-ui-redesign.md`, `curated-creator-consolidation-hotposts-fix.md`.

## Epic DoD
- [x] `creators`/`profiles` + schema catalog + backfill migration, idempotent.
- [ ] (open) Full creators UI polish + cross-platform socials surfacing.
