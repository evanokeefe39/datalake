"""Cross-domain shared dimensions and views."""

from .asset_checks import serving_checks
from .assets import assets  # noqa: F401

__all__ = ["assets", "serving_checks"]
