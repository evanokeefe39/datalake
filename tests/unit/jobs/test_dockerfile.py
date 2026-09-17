"""Packaging guards for the container path.

The jobs service is a member of the repo uv workspace, so its image is built
from the REPO ROOT context and installs the workspace member — not `pip install .`
against a self-contained directory. These guards defend that arrangement:

- the Dockerfile is found by walking ancestors for the repo marker, never by a
  `parents[N]` count (a fixed depth silently reads the wrong file after a move,
  which is exactly the class of break this move could have introduced);
- the entrypoint is the module that runs the loud OPENROUTER_API_KEY startup
  check, so a container start fails as visibly as a local one.
"""

from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    """Repo root — first ancestor of this file carrying a `.git` entry."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError(f"cannot locate the repository root above {here}")


_DOCKERFILE = _repo_root() / "services" / "jobs" / "Dockerfile"


def _dockerfile_text() -> str:
    assert _DOCKERFILE.exists(), f"no Dockerfile at {_DOCKERFILE}"
    return _DOCKERFILE.read_text(encoding="utf-8")


def _instructions() -> str:
    """The Dockerfile's EXECUTABLE lines — comments stripped.

    A guard that greps the raw text matches the prose explaining WHY a
    construct is absent, and then fails on a correct file. Only instructions
    describe what the image does.
    """
    lines = []
    for raw in _dockerfile_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


def test_installs_the_workspace_member_not_a_standalone_package():
    """The image syncs the `jobs` member from the root workspace."""
    text = _instructions()
    assert "uv sync" in text, "image must install via uv (the repo forbids pip)"
    assert "--package jobs" in text, "must select the jobs workspace member"
    assert "pip install" not in text, "uv workspaces cannot be consumed by pip install"


def test_cmd_runs_main_entrypoint():
    text = _instructions()
    assert "jobs.main" in text
    # uvicorn must not be launched directly: that skips main's API key check.
    assert "uvicorn " not in text and '"uvicorn"' not in text


def test_container_binds_all_interfaces():
    # main defaults to 127.0.0.1; the container must bind 0.0.0.0 or the
    # published port mapping cannot work.
    assert "QWEN_BATCH_HOST=0.0.0.0" in _instructions()
