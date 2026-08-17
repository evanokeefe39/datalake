"""One-shot: re-materialize serving views after the creators schema change."""
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.serving.assets import (
    dim_date,
    profile_dimension,
    v_creator_outlier_rate,
    v_creator_quality,
    v_domain_coverage,
    v_engagement_outliers,
    v_outlier_posts,
    v_post_detail,
    v_quality_trend,
    v_rising_creators,
    v_signal,
)

db = DuckDBResource(database="data/state.duckdb")
ops = SQLiteResource(database="data/ops.sqlite")
ctx = build_asset_context(resources={"duckdb": db})
ctx_ops = build_asset_context(resources={"duckdb": db, "ops": ops})

dim_date(ctx)
profile_dimension(ctx_ops)
for view in (
    v_post_detail,
    v_signal,
    v_quality_trend,
    v_creator_quality,
    v_rising_creators,
    v_domain_coverage,
    v_engagement_outliers,
    v_outlier_posts,
    v_creator_outlier_rate,
):
    view(ctx)

print("serving views refreshed")
