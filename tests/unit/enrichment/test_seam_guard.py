"""ADR-0008 seam guard regression tests (Phase 5).

Proves the scanner behind ``check_enrichment_seam_purity``:

- a PURE asset containing a Gemini API call (``generate_content``) is
  REJECTED — this is the failing-regression: adding such a call to a hermetic
  module turns the registered asset check red;
- untagged ops (no ``seam`` tag) are rejected even in API-adjacent modules;
- the real enrichment package is clean: the seam-tagged
  ``harvest_gemini_batches_op`` and ``media_upload_pending_batches_op`` pass.

Synthetic modules are written to real temp files and imported with
importlib so ``inspect.getsource`` works — exactly what the scanner sees for
the installed package.
"""

from __future__ import annotations

import importlib.util
import sys
import textwrap
import uuid
from pathlib import Path

from datalake.defs.enrichment.media_upload import seam_violations


def _load_module(source: str, name: str):
    """Write ``source`` to a real temp file and import it as ``name``."""
    path = Path(__file__).parent / f"_seam_{name}_{uuid.uuid4().hex[:8]}.py"
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module, path


class TestSeamGuard:
    def test_real_package_is_clean(self):
        """The seam-tagged harvest + media ops (and everything else) pass."""
        assert seam_violations() == []

    def test_pure_asset_with_generate_content_rejected(self):
        """A pure asset calling generate_content must be flagged."""
        module, path = _load_module(
            """
            from dagster import asset


            @asset
            def rogue_gold():
                from google.genai import Client

                client = Client(api_key="x")
                client.models.generate_content(model="m", contents="hi")
                return None
            """,
            "rogue_pure_asset",
        )
        try:
            violations = seam_violations(pure_modules=[module])
            assert violations, "API call in a pure asset was not rejected"
            assert any("generate_content" in v for v in violations)
            assert any("rogue_pure_asset" in v for v in violations)
        finally:
            path.unlink()

    def test_pure_module_with_untagged_op_rejected(self):
        """An op without the ADR-0008 seam tag is a violation."""
        module, path = _load_module(
            """
            from dagster import op


            @op
            def rogue_op(context):
                return None
            """,
            "rogue_untagged_op",
        )
        try:
            violations = seam_violations(pure_modules=[], extra_modules=[module])
            assert violations, "untagged op was not rejected"
            assert any("missing the ADR-0008 seam tag" in v for v in violations)
        finally:
            path.unlink()

    def test_seam_tagged_op_passes(self):
        """A properly tagged op introduces no violation."""
        module, path = _load_module(
            """
            from dagster import op

            SEAM_TAGS = {"adr": "0008", "seam": "enrichment-api"}


            @op(tags=SEAM_TAGS)
            def proper_seam_op(context):
                return None
            """,
            "proper_seam_op",
        )
        try:
            violations = seam_violations(pure_modules=[], extra_modules=[module])
            assert violations == []
        finally:
            path.unlink()

    def test_registered_asset_check_reflects_scanner(self):
        """check_enrichment_seam_purity is registered on the gold asset and
        passes on the current package state."""
        from dagster import AssetCheckResult

        from datalake.defs.enrichment.assets import (
            ENRICHMENT_CHECKS,
            check_enrichment_seam_purity,
        )

        assert check_enrichment_seam_purity in ENRICHMENT_CHECKS
        result = check_enrichment_seam_purity()
        assert isinstance(result, AssetCheckResult)
        assert result.passed
