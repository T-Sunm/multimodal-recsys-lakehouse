-- Silver user sequences: sliding window tối đa max_seq_len sự kiện gần nhất
-- Output dùng cho gold_training_samples và inference
{% set max_len = var('max_seq_len', 50) %}

WITH sorted_events AS (
    SELECT
        user_id,
        item_id,
        timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY timestamp DESC
        ) AS recency_rank
    FROM {{ ref('silver_interactions') }}
),

windowed AS (
    SELECT user_id, item_id, timestamp
    FROM sorted_events
    WHERE recency_rank <= {{ max_len }}
),

full_counts AS (
    SELECT user_id, COUNT(*) AS total_events
    FROM {{ ref('silver_interactions') }}
    GROUP BY user_id
),

aggregated AS (
    SELECT
        w.user_id,
        COLLECT_LIST(w.item_id)   AS item_sequence,
        COLLECT_LIST(w.timestamp) AS timestamp_sequence,
        COUNT(*)                  AS seq_len
    FROM (
        SELECT * FROM windowed ORDER BY user_id, timestamp ASC
    ) w
    GROUP BY w.user_id
)

SELECT
    a.user_id,
    a.item_sequence,
    a.timestamp_sequence,
    a.seq_len,
    f.total_events,
    CASE
        WHEN f.total_events < 5  THEN 'cold'
        WHEN f.total_events < 20 THEN 'warm'
        ELSE 'hot'
    END                           AS user_tier,
    current_timestamp()           AS updated_at
FROM aggregated a
JOIN full_counts f ON a.user_id = f.user_id
