"""Filesystem resolution: one root, one scheme, selected by configuration.

A caller asks for a filesystem and gets the one the environment names — a local
directory today, an R2/S3 bucket under `storage-migration`. No caller reads the
transport directly, so the migration is a settings change rather than a rewrite
of every media path.

Not implemented in this branch: `storage-migration` lands the R2 wiring and
repoints `engine/media.py` and `platform/paths.py` at it. The signature here is
the contract those callers will be written against.
"""

from __future__ import annotations

from typing import Any

#: Environment variable naming the storage root. Empty/unset means "local".
STORAGE_ROOT_ENV = "STORAGE_ROOT"

#: Environment variable naming the fsspec protocol ("" → local filesystem).
STORAGE_PROTOCOL_ENV = "STORAGE_PROTOCOL"


def storage_root() -> str:
    """Return the configured storage root.

    Precondition: none.
    Postcondition: returns the `STORAGE_ROOT` value, or an empty string when it
    is unset — callers treat empty as "the local data directory".
    """
    import os

    return os.environ.get(STORAGE_ROOT_ENV, "")


def filesystem(**options: Any) -> Any:
    """Return an fsspec filesystem for the configured root.

    Precondition: `fsspec` is importable.
    Postcondition: returns a filesystem handle. This branch does not implement
    the R2 branch of the resolution; it raises rather than silently returning a
    local handle for a remote root, because a silent fallback would write the
    corpus to the wrong side of the migration.
    """
    protocol = __import__("os").environ.get(STORAGE_PROTOCOL_ENV, "")
    if protocol:
        raise NotImplementedError(
            f"{STORAGE_PROTOCOL_ENV}={protocol!r} is not wired yet: "
            "the R2 transport lands with storage-migration"
        )

    import fsspec

    return fsspec.filesystem("file", **options)
