# Issues & deferred work

Issue tracking is local — this file, not GitHub Issues.

## Active

### Scaffold complete — assets not yet implemented
- Definitions module compiles, dagster.yaml set, resources wired
- `defs/{bronze,silver,gold,serving}/` stubs exist but are empty
- No assets registered yet in `definitions.py`

### Status
- `dg dev` boots, no-op definitions validate
- CI runs `dagster definitions validate` — will catch broken definitions

## Deferred

### Phase 1: Wire IG library imports
See `tasks/todo.md` for full plan. The old `ig_pipeline` functions need thin wrappers so assets can call them.

### Phase 2–7: Asset implementation
Full 8-asset pipeline (bronze → silver → gold → serving) plus sensor, schedule, tests, cutover.

## Won't fix

- GitHub Issues — local tracking only
