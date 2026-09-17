"""The ``creator_merges`` ledger — operations are NOT authored here yet.

The table's DDL is `schema.py`'s (`creator_merges`); this module is a placeholder
naming where its *operations* land, so the file tree is not mistaken for a
finished extraction.

The operations themselves — recording a merge, reversing one via `--undo`, and
the surviving-creator lookup the roster reconciles against — are authored in the
`services-extraction` workstream out of
`migrations/migrate_curated_creator_merge.py`, which is where the logic lives
today. That migration is the specification for this module.
"""

from __future__ import annotations
