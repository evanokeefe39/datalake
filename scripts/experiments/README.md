# experiments

Historical facet-schema selection experiments — not ops, not maintained.

| Script | What it is |
|---|---|
| `facet_experiment.py` | Original facet-schema selection experiment over sampled posts (shared helpers: `sample_posts`, `resolve_post_media`). |
| `facet_menu_experiment.py` | Follow-up experiment testing a menu-style facet enumeration. |
| `facet_summary_spike.py` | Spike for visual summaries. |

These are kept for provenance only. They target the pre-ADR-0011 gemini-batch
machinery (`gemini_batch`, `_DEFAULT_GEMINI_MODEL`) and are not wired into the
pipeline. Do not import from them in ops code.
