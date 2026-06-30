"""Instagram domain — assets, configs, serving views."""

from .assets import ig_posts_gld, ig_posts_raw, ig_posts_slv
from .config import GoldConfig, ScrapeConfig

__all__ = ["ig_posts_gld", "ig_posts_raw", "ig_posts_slv", "ScrapeConfig", "GoldConfig"]
