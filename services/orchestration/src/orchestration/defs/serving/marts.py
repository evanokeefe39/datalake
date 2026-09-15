"""Analytic marts — the four cross-domain answers the owner asked for.

Each mart COMPOSES the canonical metric views; none restates a tier
bucket, momentum constant, baseline or z-score."""


from dagster import AssetKey, asset
from dagster_duckdb import DuckDBResource


@asset(
    name="gold_post_enrichment",
    group_name="serving",
    description=(
        "Wide per-post enrichment mart: canonical engagement metrics + "
        "classification shape + all five silver channel outputs + provenance. "
        "PK (post_id, platform)."
    ),
    deps=[
        AssetKey(["v_post_metrics"]),
        AssetKey(["v_post_detail"]),
        AssetKey(["silver_visual_annotations"]),
        AssetKey(["silver_visual_summaries"]),
        AssetKey(["silver_audio_transcripts"]),
        AssetKey(["silver_text_annotations"]),
        AssetKey(["silver_text_summaries"]),
    ],
)
def gold_post_enrichment(duckdb: DuckDBResource) -> None:
    """AC1 — the wide per-post shape.

    Every metric is SELECTED from a canonical view (``v_post_metrics`` for
    engagement, ``v_post_detail`` for the typed classification columns);
    the five silver channel tables join on ``(post_id, platform)`` — the
    platform key, never ``domain``. ``platform`` is sourced from
    ``dim_profile.channel`` via ``v_post_detail`` (no literal restated).
    Creator identity (``creator_id``/``owner_username``) is carried so
    creator-grain consumers can read the mart without re-joining
    ``v_post_metrics``.
    LEFT JOINs throughout: a post missing a channel output still appears.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW gold_post_enrichment AS
            SELECT
                pm.post_id,
                pm.creator_id,
                pm.owner_username,
                pd.channel                             AS platform,
                -- Engagement metrics — canonical (v_post_metrics).
                pm.likes_count,
                pm.comments_count,
                pm.video_view_count,
                pm.timestamp,
                pm.likes_zscore,
                pm.comments_zscore,
                pm.views_zscore,
                pm.engagement_score,
                pm.sigma_tier,
                pm.is_standout,
                pm.is_hot,
                pm.relative_performance,
                pm.breakout_multiple,
                -- Content classification shape — canonical (v_post_detail).
                pd.gold_domain,
                pd.gold_subdomain,
                pd.gold_topic,
                pd.gold_subtopic,
                pd.content_type,
                pd.style,
                pd.format,
                pd.is_educational,
                pd.is_actionable,
                pd.admiralty,
                -- Per-pass provenance (envelope metadata, one prefix/channel).
                sva.provider        AS visual_provider,
                sva.model           AS visual_model,
                sva.run_id          AS visual_run_id,
                sva.schema_version  AS visual_schema_version,
                sva.analysed_at     AS visual_analysed_at,
                svs.provider        AS visual_summary_provider,
                svs.model           AS visual_summary_model,
                svs.run_id          AS visual_summary_run_id,
                svs.analysed_at     AS visual_summary_analysed_at,
                sat.provider        AS transcript_provider,
                sat.model           AS transcript_model,
                sat.run_id          AS transcript_run_id,
                sat.analysed_at     AS transcript_analysed_at,
                sta.provider        AS text_provider,
                sta.model           AS text_model,
                sta.run_id          AS text_run_id,
                sta.schema_version  AS text_schema_version,
                sta.analysed_at     AS text_analysed_at,
                sts.provider        AS text_summary_provider,
                sts.model           AS text_summary_model,
                sts.run_id          AS text_summary_run_id,
                sts.analysed_at     AS text_summary_analysed_at,
                -- Visual annotations (visual pass).
                sva.face_present,
                sva.value_medium,
                sva.brand_logos_json,
                sva.text_overlay_present,
                sva.on_screen_claim,
                -- Visual summaries (same visual pass; overall + per-image).
                svs.content_summary,
                svs.image_summaries_json,
                -- Transcript (whisper; explicit status, never a silent NULL).
                sat.transcript,
                sat.transcript_status,
                sat.audio_present,
                sat.asr_model,
                sat.language,
                -- Text annotations (text-LLM pass).
                sta.hook_content,
                sta.hook_type,
                sta.is_sponsored,
                sta.sponsorship_signal,
                sta.claimed_results,
                sta.cta_type,
                sta.audience_named,
                sta.value_depth,
                sta.replicable_tactic,
                sta.hashtag_strategy,
                sta.evidence,
                sta.brand_safety_json,
                -- Text summary (text-LLM pass).
                sts.transcript_summary
            FROM v_post_metrics pm
            JOIN v_post_detail pd
                ON pd.post_id = pm.post_id
            LEFT JOIN silver_visual_annotations sva
                ON sva.post_id = pm.post_id AND sva.platform = pd.channel
            LEFT JOIN silver_visual_summaries svs
                ON svs.post_id = pm.post_id AND svs.platform = pd.channel
            LEFT JOIN silver_audio_transcripts sat
                ON sat.post_id = pm.post_id AND sat.platform = pd.channel
            LEFT JOIN silver_text_annotations sta
                ON sta.post_id = pm.post_id AND sta.platform = pd.channel
            LEFT JOIN silver_text_summaries sts
                ON sts.post_id = pm.post_id AND sts.platform = pd.channel
        """)


@asset(
    name="gold_creator_performance",
    group_name="serving",
    description=(
        "Q1 mart — who performs well in X domain. Composes v_creator_profile "
        "+ v_post_metrics + v_post_follower_context; PK (creator_id, platform)."
    ),
    deps=[
        AssetKey(["v_creator_profile"]),
        AssetKey(["v_post_metrics"]),
        AssetKey(["v_post_follower_context"]),
        AssetKey(["v_post_detail"]),
    ],
)
def gold_creator_performance(duckdb: DuckDBResource) -> None:
    """AC2 — one row per creator (no row inflation: followers/median are
    per-creator aggregates, joined 1:1 onto ``v_creator_profile``).

    ``post_count``/``avg_engagement_score``/``standout_count``/
    ``momentum_ratio``/``is_rising``/``dominant_domain`` are SELECTED from
    ``v_creator_profile`` — never re-aggregated or re-gated. The mart adds
    ONLY: the at-post-time ``follower_count``/``follower_tier`` (latest
    observation per creator, from ``v_post_follower_context`` — the tier
    buckets stay canonical), the ``median_engagement_score`` (median over
    the canonical per-post ``engagement_score``), and ``standout_rate``
    (the ratio of the canonical counts). ``platform`` is sourced from
    ``dim_profile.channel`` via ``v_post_detail``.
    Filter by ``dominant_domain`` for "who performs well in X domain".
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW gold_creator_performance AS
            WITH channel AS (
                SELECT
                    pd.creator_id,
                    MIN(pd.channel) AS platform
                FROM v_post_detail pd
                WHERE pd.creator_id IS NOT NULL
                GROUP BY pd.creator_id
            ),
            follower AS (
                SELECT
                    pm.creator_id,
                    arg_max(fc.followers_count, fc.follower_observed_at)
                        AS follower_count,
                    arg_max(fc.follower_tier, fc.follower_observed_at)
                        AS follower_tier
                FROM v_post_metrics pm
                JOIN v_post_follower_context fc
                    ON fc.post_id = pm.post_id
                WHERE pm.creator_id IS NOT NULL
                  AND fc.follower_observed_at IS NOT NULL
                GROUP BY pm.creator_id
            ),
            median_score AS (
                SELECT
                    creator_id,
                    median(engagement_score) AS median_engagement_score
                FROM v_post_metrics
                WHERE creator_id IS NOT NULL
                  AND engagement_score IS NOT NULL
                GROUP BY creator_id
            )
            SELECT
                cp.creator_id,
                cp.creator_name,
                ch.platform,
                f.follower_count,
                f.follower_tier,
                cp.total_posts            AS post_count,
                cp.standout_count,
                cp.standout_count / NULLIF(cp.total_posts, 0)
                                          AS standout_rate,
                m.median_engagement_score,
                cp.avg_engagement_score,
                cp.avg_likes,
                cp.max_likes,
                cp.momentum_ratio,
                cp.is_rising,
                cp.dominant_domain
            FROM v_creator_profile cp
            LEFT JOIN channel ch
                ON ch.creator_id = cp.creator_id
            LEFT JOIN follower f
                ON f.creator_id = cp.creator_id
            LEFT JOIN median_score m
                ON m.creator_id = cp.creator_id
        """)


@asset(
    name="gold_content_shape_performance",
    group_name="serving",
    description=(
        "Q2 mart — what content shape performs. LONG form keyed "
        "(platform, domain, topic, follower_tier, facet_name, facet_value); "
        "facets from the silver annotations + classification, measured via "
        "the canonical views."
    ),
    deps=[
        AssetKey(["v_post_metrics"]),
        AssetKey(["v_post_detail"]),
        AssetKey(["v_post_follower_context"]),
        AssetKey(["silver_visual_annotations"]),
        AssetKey(["silver_text_annotations"]),
    ],
)
def gold_content_shape_performance(duckdb: DuckDBResource) -> None:
    """AC3 — long-form facet cells so facet-schema evolution is new rows,
    not a migration.

    Each enrichment facet is unpivoted to ``(facet_name, facet_value)``
    (one value per post per facet ⇒ no double counting), attached to the
    canonical ``follower_tier`` (``v_post_follower_context``) and measured
    with the canonical ``engagement_score``/``is_standout``
    (``v_post_metrics``). ``avg_engagement_z`` is the mean of the canonical
    per-post score — not a re-derivation. ``lift_vs_slice_baseline`` is the
    cell mean over the same (platform, domain, topic, tier, facet) slice's
    overall mean — a mart-local projection over canonical values, no
    constant. The grain includes ``platform`` (deliberate deviation from
    enrichment.md §5.3, which omits it) — facets carry platform, and a
    domain-less grain would merge posts across platforms.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW gold_content_shape_performance AS
            WITH facets AS (
                -- Visual annotations (visual pass).
                SELECT post_id, platform,
                       'value_medium' AS facet_name,
                       value_medium   AS facet_value
                FROM silver_visual_annotations
                WHERE value_medium IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'face_present',
                       face_present::VARCHAR
                FROM silver_visual_annotations
                WHERE face_present IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'text_overlay_present',
                       text_overlay_present::VARCHAR
                FROM silver_visual_annotations
                WHERE text_overlay_present IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'on_screen_claim',
                       on_screen_claim::VARCHAR
                FROM silver_visual_annotations
                WHERE on_screen_claim IS NOT NULL
                -- Text annotations (text-LLM pass).
                UNION ALL
                SELECT post_id, platform, 'hook_type', hook_type
                FROM silver_text_annotations WHERE hook_type IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'is_sponsored',
                       is_sponsored::VARCHAR
                FROM silver_text_annotations
                WHERE is_sponsored IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'cta_type', cta_type
                FROM silver_text_annotations WHERE cta_type IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'value_depth', value_depth
                FROM silver_text_annotations WHERE value_depth IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'replicable_tactic', replicable_tactic
                FROM silver_text_annotations
                WHERE replicable_tactic IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'audience_named',
                       audience_named::VARCHAR
                FROM silver_text_annotations
                WHERE audience_named IS NOT NULL
                UNION ALL
                SELECT post_id, platform, 'claimed_results',
                       claimed_results::VARCHAR
                FROM silver_text_annotations
                WHERE claimed_results IS NOT NULL
                -- Classification metadata (canonical v_post_detail columns).
                UNION ALL
                SELECT post_id, channel, 'content_type', content_type
                FROM v_post_detail WHERE content_type IS NOT NULL
                UNION ALL
                SELECT post_id, channel, 'format', format
                FROM v_post_detail WHERE format IS NOT NULL
                UNION ALL
                SELECT post_id, channel, 'style', style
                FROM v_post_detail WHERE style IS NOT NULL
                UNION ALL
                SELECT post_id, channel, 'admiralty', admiralty
                FROM v_post_detail WHERE admiralty IS NOT NULL
                UNION ALL
                SELECT post_id, channel, 'is_educational',
                       is_educational::VARCHAR
                FROM v_post_detail WHERE is_educational IS NOT NULL
            ),
            facet_posts AS (
                SELECT
                    f.platform,
                    pd.gold_domain,
                    pd.gold_topic,
                    fc.follower_tier,
                    f.facet_name,
                    f.facet_value,
                    pm.engagement_score,
                    pm.is_standout
                FROM facets f
                JOIN v_post_detail pd
                    ON pd.post_id = f.post_id AND pd.channel = f.platform
                JOIN v_post_metrics pm
                    ON pm.post_id = f.post_id
                LEFT JOIN v_post_follower_context fc
                    ON fc.post_id = f.post_id
                WHERE pd.gold_domain IS NOT NULL
                  AND pd.gold_topic IS NOT NULL
            ),
            cells AS (
                SELECT
                    platform,
                    gold_domain, gold_topic, follower_tier,
                    facet_name, facet_value,
                    COUNT(*)               AS n_posts,
                    AVG(engagement_score)  AS avg_engagement_z,
                    AVG(is_standout)       AS standout_rate
                FROM facet_posts
                GROUP BY platform, gold_domain, gold_topic, follower_tier,
                         facet_name, facet_value
            ),
            slices AS (
                SELECT
                    platform,
                    gold_domain, gold_topic, follower_tier, facet_name,
                    AVG(engagement_score) AS slice_avg_engagement_z
                FROM facet_posts
                GROUP BY platform, gold_domain, gold_topic, follower_tier,
                         facet_name
            )
            SELECT
                c.platform,
                c.gold_domain,
                c.gold_topic,
                c.follower_tier,
                c.facet_name,
                c.facet_value,
                c.n_posts,
                c.avg_engagement_z,
                c.standout_rate,
                c.avg_engagement_z
                    / NULLIF(s.slice_avg_engagement_z, 0)
                    AS lift_vs_slice_baseline
            FROM cells c
            JOIN slices s
                ON  s.platform       = c.platform
                AND s.gold_domain    = c.gold_domain
                AND s.gold_topic     = c.gold_topic
                AND s.follower_tier IS NOT DISTINCT FROM c.follower_tier
                AND s.facet_name     = c.facet_name
        """)


@asset(
    name="gold_top_posts",
    group_name="serving",
    description=(
        "Q3 mart — what performs across ALL domains: rank/percentile over "
        "the canonical engagement score, joined to the full content shape. "
        "PK (post_id, platform)."
    ),
    deps=[AssetKey(["gold_post_enrichment"])],
)
def gold_top_posts(duckdb: DuckDBResource) -> None:
    """AC4 — a thin projection over ``gold_post_enrichment``: every scored
    post with its cross-domain rank/percentile and the full qualitative
    shape (summary/transcript) for reading. No metric is re-derived —
    ``engagement_score`` flows straight from ``v_post_metrics``; consumers
    filter ``overall_rank``/``is_hot`` rather than this mart ranking again.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW gold_top_posts AS
            SELECT
                gpe.*,
                RANK() OVER (
                    ORDER BY gpe.engagement_score DESC NULLS LAST
                ) AS overall_rank,
                PERCENT_RANK() OVER (
                    ORDER BY gpe.engagement_score DESC NULLS LAST
                ) AS overall_percentile
            FROM gold_post_enrichment gpe
            WHERE gpe.engagement_score IS NOT NULL
        """)


ASSETS: list = [
    gold_post_enrichment,
    gold_creator_performance,
    gold_content_shape_performance,
    gold_top_posts,
]
