"""Domains — self-contained asset groupings.

Each `<platform>_<stage>/` package owns one platform's assets, split into
`bnz/` (land), `slv/` (conform) and `gld/` (domain-scoped gold). Cross-domain
machinery lives in `engine/`; the semantic layer and marts live in `serving/`.

This package deliberately imports nothing: every `__init__.py` under `defs/`
is a docstring and nothing else, so importing one module never drags in its
siblings' dependencies (ADR-0015).
"""
