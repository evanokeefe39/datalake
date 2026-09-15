"""youtube_enriched/bnz — bronze layer (skeleton, empty by design).

Verbatim landing of provider responses is generic and lives in
engine/landing.py — keyed (post_id, platform, workload), it needs no
per-platform code. This folder exists only for shape uniformity; it should
stay empty unless a genuinely YouTube-specific landing concern appears.
"""
