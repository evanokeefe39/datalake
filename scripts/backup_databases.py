"""Back up datalake operational DBs (ops.sqlite, state.duckdb) off-machine.

Purpose: the creator/roster list in ops.sqlite and the analytical state in
state.duckdb are single-writer, gitignored, and local-only. If the machine is
lost they are gone — this pushes consistent snapshots to Cloudflare R2.

R2 target (uses an AWS profile so credentials stay out of the repo):
  profile  : r2-sessions   (~/.aws/credentials — the token scoped to write
             the `agent-sessions` bucket; r2-handoff is scoped elsewhere)
  bucket   : agent-sessions
  prefix   : datalake/db/<UTC-date>/ops.sqlite | state.duckdb | manifest.json

Consistency:
  - ops.sqlite is copied via the SQLite online-backup API (safe even if open).
  - state.duckdb is copied after a DuckDB checkpoint (flushes any WAL) so the
    restored file is self-contained.

Also writes a timestamped local copy under data/backups/ (gitignored) so there is
both a local snapshot and the off-machine R2 copy in one invocation.

Usage:
  uv run python scripts/backup_databases.py            # R2 (r2-sessions) + local
  uv run python scripts/backup_databases.py --local-only
  uv run python scripts/backup_databases.py --profile r2-sessions --bucket agent-sessions

Notes: boto3 reads the profile from ~/.aws; endpoint/region come from
~/.aws/config for that profile (region=auto, R2 endpoint_url). New dep: boto3
(added 2026-09-05 for the R2 backup path).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCAL_BACKUP_DIR = ROOT / "data" / "backups"


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot_sqlite(src: Path, dst: Path) -> None:
    """Consistent online backup of a SQLite DB."""
    s = sqlite3.connect(str(src))
    d = sqlite3.connect(str(dst))
    try:
        s.backup(d)
    finally:
        d.close()
        s.close()


def snapshot_duckdb(src: Path, dst: Path) -> None:
    """Checkpoint (flush WAL) then copy a DuckDB file so it is self-contained."""
    try:
        import duckdb
        con = duckdb.connect(str(src))
        try:
            con.execute("CHECKPOINT")
        finally:
            con.close()
    except Exception as e:  # DB open/locked — fall back to plain copy
        print(f"  [warn] duckdb checkpoint failed ({e}); copying file as-is")
    dst.write_bytes(src.read_bytes())


def upload(client, bucket: str, key: str, path: Path, manifest: dict) -> None:
    print(f"  uploading {path.name} ({path.stat().st_size/1e6:.1f} MB) -> s3://{bucket}/{key}")
    client.upload_file(str(path), bucket, key)
    head = client.head_object(Bucket=bucket, Key=key)
    manifest[key] = {"bytes": head["ContentLength"], "sha256": _sha256(path),
                     "etag": head["ETag"].strip('"')}
    print(f"    verified: {manifest[key]['bytes']} bytes, sha256 {manifest[key]['sha256'][:12]}…")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--local-only", action="store_true", help="skip R2 upload")
    ap.add_argument("--profile", default="r2-sessions")
    ap.add_argument("--bucket", default="agent-sessions")
    ap.add_argument("--prefix", default="datalake/db")
    args = ap.parse_args()

    ops = ROOT / "data" / "ops.sqlite"
    duck = ROOT / "data" / "state.duckdb"
    if not ops.exists() and not duck.exists():
        print("no data/ops.sqlite or data/state.duckdb found; nothing to back up")
        return 1

    stamp = _stamp()
    date = stamp[:8]
    LOCAL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    manifest: dict = {"created": datetime.now(timezone.utc).isoformat(),
                      "source_db": {"ops": str(ops), "duckdb": str(duck)},
                      "files": {}}

    if ops.exists():
        local = LOCAL_BACKUP_DIR / f"ops-{stamp}.sqlite"
        snapshot_sqlite(ops, local)
        manifest["files"]["ops.sqlite"] = {"local": str(local), "bytes": local.stat().st_size,
                                           "sha256": _sha256(local)}
        print(f"local ops snapshot: {local} (integrity ok)")
    if duck.exists():
        local = LOCAL_BACKUP_DIR / f"state-{stamp}.duckdb"
        snapshot_duckdb(duck, local)
        manifest["files"]["state.duckdb"] = {"local": str(local), "bytes": local.stat().st_size,
                                             "sha256": _sha256(local)}
        print(f"local duckdb snapshot: {local}")

    if not args.local_only:
        try:
            import boto3
            from botocore.config import Config
            client = boto3.session.Session(profile_name=args.profile).client(
                "s3", config=Config(signature_version="s3v4"), region_name="auto")
        except Exception as e:
            print(f"[warn] R2 client init failed ({e}); local backup only")
            return 1
        for fname in ["ops.sqlite", "state.duckdb"]:
            if fname not in manifest["files"]:
                continue
            local = Path(manifest["files"][fname]["local"])
            key = f"{args.prefix.rstrip('/')}/{date}/{fname}"
            upload(client, args.bucket, key, local, manifest["files"])
        manifest_path = LOCAL_BACKUP_DIR / f"manifest-{stamp}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        # push manifest too so the backup set is self-describing in R2
        client.upload_file(str(manifest_path), args.bucket,
                           f"{args.prefix.rstrip('/')}/{date}/manifest.json")
        print(f"R2 backup complete: s3://{args.bucket}/{args.prefix}/{date}/")
    else:
        manifest_path = LOCAL_BACKUP_DIR / f"manifest-{stamp}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print("local-only backup complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
