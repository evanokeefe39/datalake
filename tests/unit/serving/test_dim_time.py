"""Tests for the ``dim_time`` date dimension (planned asset).

This asset does not exist yet — tests are skippable placeholders that document
the expected contract.

Gap-fills per test-hardening plan:
- Date spine continuity (no missing dates)
- Date range bounds correct
"""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="dim_time asset not yet implemented")
def test_date_spine_continuity():
    """``dim_time`` contains every calendar day in [min_date, max_date]
    without gaps.
    """
    ...


@pytest.mark.skip(reason="dim_time asset not yet implemented")
def test_date_range_bounds():
    """Date range covers at least the expected window (e.g. 2020-01-01
    through today + 1 year).
    """
    ...
