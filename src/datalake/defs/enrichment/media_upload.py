"""ADR-0008 seam-purity scanner for the enrichment domain.

The Gemini media pre-upload machinery that previously lived here
(``upload_media_for_pending_batches``, ``pending_media_candidates`` — a
``batch_items`` reader and a Gemini File-API transport) is RETIRED with the
queue and the Gemini path (ADR-0012, W-FREEZE). Media on the target path
resolves from the scrape-time byte cache at submit time; no provider
transport is named outside the adapter layer.

This module keeps ONLY the seam guard that backs the
``check_enrichment_seam_purity`` asset check: pure enrichment modules must
contain no provider API markers, and every op in the enrichment package
must carry the ``{adr: 0008, seam: enrichment-api}`` tag.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil

from dagster import OpDefinition

# ADR-0008 seam tag — every op that may touch a provider API carries this.
SEAM_TAGS = {"adr": "0008", "seam": "enrichment-api"}

# Source markers that indicate a provider API call. Any hit inside a "pure"
# enrichment module is an ADR-0008 violation.
_API_MARKERS: tuple[str, ...] = (
    "generate_content",
    "GeminiClient",
    "files.upload",
    "batches.get",
    "batches.submit",
    "gemini_batch.",
    "lookup_or_upload_all",
    ".analyze(",
)

# Enrichment modules that must stay hermetic (no API calls, no ops).
_PURE_MODULES: tuple[str, ...] = (
    "datalake.defs.enrichment.assets",
    "datalake.defs.enrichment.registry",
    "datalake.defs.enrichment.prompts",
)


def _module_ops(module) -> list[OpDefinition]:
    return [
        attr
        for attr in vars(module).values()
        if isinstance(attr, OpDefinition)
    ]


def seam_violations(
    pure_modules: list | None = None,
    extra_modules: list | None = None,
) -> list[str]:
    """ADR-0008 seam purity scan over the enrichment domain.

    Two rules:

    1. Pure modules (assets/registry/prompts by default) must contain no
       provider API markers in their source and must not define Dagster ops —
       this is what rejects a ``generate_content`` call added to a pure asset.
    2. Every op in the enrichment package (plus ``extra_modules``, for tests)
       must carry the ``seam: enrichment-api`` tag — an untagged op is a seam
       violation even if its API use is currently benign.

    ``pure_modules``/``extra_modules`` accept any module object so tests can
    inject synthetic modules without editing the package.
    """
    violations: list[str] = []

    modules = pure_modules
    if modules is None:
        modules = [importlib.import_module(m) for m in _PURE_MODULES]
    for module in modules:
        src = inspect.getsource(module)
        for marker in _API_MARKERS:
            if marker in src:
                violations.append(
                    f"{module.__name__}: hermetic module contains API marker "
                    f"'{marker}'"
                )
        for op_def in _module_ops(module):
            violations.append(
                f"{module.__name__}: op '{op_def.name}' defined in a "
                "hermetic module"
            )

    # NOTE: `import datalake.defs.enrichment as pkg` fails repo-wide —
    # datalake/__init__.py shadows the `defs` attribute with the Definitions
    # object — so resolve the package through importlib instead.
    pkg = importlib.import_module("datalake.defs.enrichment")
    walk = extra_modules or []
    try:
        for mod_info in pkgutil.iter_modules(pkg.__path__):
            walk.append(importlib.import_module(f"{pkg.__name__}.{mod_info.name}"))
    except Exception as exc:  # pragma: no cover - defensive
        violations.append(f"enrichment package walk failed: {exc}")

    for module in walk:
        for op_def in _module_ops(module):
            tags = op_def.tags or {}
            if tags.get("seam") != "enrichment-api":
                violations.append(
                    f"{module.__name__}: op '{op_def.name}' is missing the "
                    f"ADR-0008 seam tag {SEAM_TAGS}"
                )
    return violations
