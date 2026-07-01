"""Instagram domain — assets, configs, serving views."""

from .asset_checks import (
    ig_checks,
)
from .assets import ig_posts_gld, ig_posts_raw, ig_posts_slv
from .config import GoldConfig, ScrapeConfig

__all__ = [
    "ig_checks",
    "ig_posts_gld",
    "ig_posts_raw",
    "ig_posts_slv",
    "ScrapeConfig",
    "GoldConfig",
]
