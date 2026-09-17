"""Content-addressed key scheme for stored bytes.

The scheme is `sha256(url)` hex — the same digest the media cache has always
keyed on, promoted to the address of the object itself. A key is therefore
stable across hosts, re-scrapes and CDN rotations, and two callers that resolve
the same source URL land on the same object without coordinating.
"""

from __future__ import annotations

import hashlib

#: Prefix separating the key space from any future namespaced scheme.
KEY_SCHEME = "sha256"


def url_hash(url: str) -> str:
    """Return the content-address key for a source URL.

    Precondition: `url` is a non-empty string identifying the source object.
    Postcondition: returns a 64-character lowercase hex digest; the same `url`
    always maps to the same key and different URLs collide only by sha256
    collision.
    """
    if not url:
        raise ValueError("url must be a non-empty string")
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def key_to_relative_path(key: str, *, extension: str = "") -> str:
    """Fan a key into a two-level directory tree: `ab/cd/<key><ext>`.

    Precondition: `key` is a hex digest from `url_hash`; `extension` is empty or
    begins with ".".
    Postcondition: returns a POSIX-style relative path. The two-level fan keeps
    any single directory under a filesystem entry-count that degrades listing.
    """
    if len(key) < 4:
        raise ValueError(f"key too short to fan out: {key!r}")
    if extension and not extension.startswith("."):
        raise ValueError(f"extension must start with '.' or be empty: {extension!r}")
    return f"{key[:2]}/{key[2:4]}/{key}{extension}"
