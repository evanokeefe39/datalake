"""youtube_enriched — future domain package (skeleton, pre-reorg).

Will hold YouTube enrichment PAYLOADS only: prompts.py, schemas.py (output
schemas), conform/ mappings (per-workload JSON→column maps), checks.py.

Hard rule: NO engine code here. submit/harvest/sensor/partitions/landing are
written once in defs/engine/ and keyed (post_id, platform, workload) — this
package adds prompts and mappings, never orchestration. If you find yourself
writing a poll loop or a submit function in this package, the abstraction
has leaked: stop and fix engine/ instead.
"""
