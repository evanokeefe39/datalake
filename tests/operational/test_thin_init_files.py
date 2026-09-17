"""Every `__init__.py` under the orchestration and package trees is a docstring.

ADR-0015's convention: an `__init__.py` may hold a docstring and nothing else —
no imports, no assignments, no side effects. Without this, a package `__init__`
silently becomes a facade that drags its siblings' dependencies into every
importer, and the import graph stops describing what a reader sees.

This is an AST check rather than a source grep, so a docstring mentioning
"import" or a commented-out line cannot fail it, and a real `import` cannot hide
behind formatting.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# The trees the convention covers. `services/jobs` and `services/dashboard` land
# under services-extraction; they are picked up by the glob when they do.
INIT_GLOBS = (
    "services/*/src/**/__init__.py",
    "packages/*/src/**/__init__.py",
)

#: Statement types an `__init__.py` MAY contain.
ALLOWED = (ast.Expr, ast.ImportFrom)


def _init_files() -> list[Path]:
    found: list[Path] = []
    for pattern in INIT_GLOBS:
        found.extend(sorted(REPO_ROOT.glob(pattern)))
    return found


def _is_docstring_only(tree: ast.Module) -> tuple[bool, str]:
    """Return (ok, reason) for one parsed `__init__.py`."""
    body = [n for n in tree.body if not isinstance(n, ast.Pass)]
    for node in body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                continue  # the module docstring
            return False, f"bare literal {node.value.value!r} at line {node.lineno}"
        return False, f"{type(node).__name__} at line {node.lineno}"
    return True, ""


def test_init_files_exist() -> None:
    """Guard the guard: an empty glob would make every case below vacuous."""
    files = _init_files()
    assert files, f"no __init__.py matched {INIT_GLOBS} under {REPO_ROOT}"


@pytest.mark.parametrize("path", _init_files(), ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_init_is_docstring_only(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    ok, reason = _is_docstring_only(tree)
    assert ok, (
        f"{path.relative_to(REPO_ROOT)} contains {reason}. An __init__.py may hold "
        "only a docstring (ADR-0015): importing a module must not pull in its "
        "siblings' dependencies."
    )
