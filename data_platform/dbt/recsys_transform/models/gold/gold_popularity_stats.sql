-- Gold popularity stats: reranking signal cho inference
-- popularity_score = 0.3 * norm_like + 0.3 * norm_view + 0.4 * norm_freq
WITH interaction_freq AS (
    SELECT
        item_id,
        COUNT(*)                AS interaction_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM {{ ref('silver_interactions') }}
    GROUP BY item_id
),

raw_stats AS (
    SELECT
        si.item_id,
        si.like_count,
        si.view_count,
        COALESCE(f.interaction_count, 0) AS interaction_count,
        COALESCE(f.unique_users, 0)      AS unique_users
    FROM {{ ref('silver_items') }} si
    LEFT JOIN interaction_freq f ON si.item_id = f.item_id
),

normalized AS (
    SELECT
        item_id,
        like_count,
        view_count,
        interaction_count,
        unique_users,
        (like_count - MIN(like_count) OVER()) /
            NULLIF(MAX(like_count) OVER() - MIN(like_count) OVER(), 0) AS norm_like,
        (view_count - MIN(view_count) OVER()) /
            NULLIF(MAX(view_count) OVER() - MIN(view_count) OVER(), 0) AS norm_view,
        (interaction_count - MIN(interaction_count) OVER()) /
            NULLIF(MAX(interaction_count) OVER() - MIN(interaction_count) OVER(), 0) AS norm_freq
    FROM raw_stats
)

SELECT
    item_id,
    like_count,
    view_count,
    interaction_count,
    unique_users,
    ROUND(
        0.3 * COALESCE(norm_like, 0) +
        0.3 * COALESCE(norm_view, 0) +
        0.4 * COALESCE(norm_freq, 0),
        6
    )                       AS popularity_score,
    current_date()          AS updated_at
FROM normalized
ORDER BY popularity_score DESC
