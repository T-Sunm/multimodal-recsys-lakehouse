-- Silver items: clean + log1p normalize like/view counts
-- Không có cover_path — visual features nằm ở silver_visual_embeddings
SELECT
    CAST(raw_item_id AS INT)                                    AS item_id,
    TRIM(raw_title)                                             AS title,
    COALESCE(CAST(raw_like_count AS BIGINT), 0)                 AS like_count,
    COALESCE(CAST(raw_view_count AS BIGINT), 0)                 AS view_count,
    LOG(1 + COALESCE(CAST(raw_like_count AS DOUBLE), 0))        AS like_log,
    LOG(1 + COALESCE(CAST(raw_view_count AS DOUBLE), 0))        AS view_log,
    ingested_at                                                 AS updated_at
FROM {{ ref('bronze_items') }}
WHERE raw_item_id IS NOT NULL
  AND raw_title   IS NOT NULL
  AND TRIM(raw_title) != ''
  AND CAST(raw_item_id AS INT) > 0
