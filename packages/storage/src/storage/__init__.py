"""Byte-storage client: one address space for every media and corpus object.

`filesystem.py` resolves a configured root (local today, R2 later) and `keys.py`
owns the content-addressed key scheme, so a caller names a *key* and never a
transport. The R2 wiring is `storage-migration`'s work; this package exists here
so that move is a config change rather than a code change.
"""
