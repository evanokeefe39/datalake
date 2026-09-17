"""`platform.paths` anchors `DATA_DIR` at the repository root, not at a fixed depth.

This is the highest-risk line in the workspace reorganization. The module reads
the environment into module constants at import, and its default data root used
to be `Path(__file__).resolve().parents[4]` — a count that silently repointed
`data/` (and the whole media corpus under it) the moment the file moved. ADR-0015
replaced the count with a walk to the `.git` marker.

The test asserts the resolved anchor, so a future move either keeps working or
fails loudly here instead of writing a corpus somewhere unintended.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_data_dir_is_repo_root_data() -> None:
    """With `IG_DATA_DIR` unset, DATA_DIR is `<repo>/data`."""
    code = (
        "import os; os.environ.pop('IG_DATA_DIR', None);"
        "from orchestration.defs.platform import paths;"
        "print(paths.DATA_DIR)"
    )
    env = {"IG_DATA_DIR": ""}
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env={**__import__("os").environ, **env},
    )
    assert proc.returncode == 0, proc.stderr
    got = Path(proc.stdout.strip())

    # `.env` may legitimately set IG_DATA_DIR; when it does, the anchor is not
    # under test. Assert only the unset case, which is what the marker walk owns.
    if got != REPO_ROOT / "data":
        assert got.name == "data", f"DATA_DIR is not a data dir: {got}"
        assert got.parent == REPO_ROOT, (
            f"DATA_DIR resolved outside the repository root: {got} "
            f"(expected {REPO_ROOT / 'data'})"
        )


def test_anchor_is_the_repo_root() -> None:
    """The private anchor resolves to the directory carrying `.git`."""
    from orchestration.defs.platform import paths

    assert (paths._PROJECT_ROOT / ".git").exists(), (
        f"_PROJECT_ROOT ({paths._PROJECT_ROOT}) carries no .git entry — the "
        "marker walk found the wrong ancestor"
    )
    assert paths._PROJECT_ROOT == REPO_ROOT.resolve(), (
        f"_PROJECT_ROOT ({paths._PROJECT_ROOT}) != repo root ({REPO_ROOT.resolve()})"
    )
