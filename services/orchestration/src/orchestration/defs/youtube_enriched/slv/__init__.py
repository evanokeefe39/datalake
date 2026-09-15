"""youtube_enriched/slv — silver enrichment payloads (skeleton).

YouTube prompts, output schemas, per-workload mappings, validation overlays
(chapter rules, long-form length bounds — NOT instagram's carousel rule),
checks.py. This is where the transcript workload registration lands
(TRANSCRIPT_WORKLOADS in the shared runtime). Engine machinery (submit,
harvest, sensor, partitions, landing, silver_rt) is shared in defs/engine/
and must never be duplicated here.
"""
