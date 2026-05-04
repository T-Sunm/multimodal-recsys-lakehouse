{% set max_len = var('max_seq_len', 50) %}

WITH user_events AS (
    SELECT
        i.user_id,
        i.item_id,
        i.timestamp,
        ROW_NUMBER() OVER (PARTITION BY i.user_id ORDER BY i.timestamp ASC) AS event_rank,
        COUNT(*)     OVER (PARTITION BY i.user_id)                          AS total_events
    FROM {{ ref('silver_interactions') }} i
    JOIN (
        SELECT user_id
        FROM {{ ref('silver_user_sequences') }}
        WHERE seq_len >= 2
    ) valid_users ON i.user_id = valid_users.user_id
),

-- Join sequence array từ silver vào từng event
events_with_seq AS (
    SELECT
        e.user_id,
        e.item_id        AS target_item,
        e.event_rank     AS target_rank,
        e.total_events,
        e.timestamp      AS target_timestamp,
        s.item_sequence  AS full_sequence   -- ARRAY<INT> đã sorted ASC từ silver
    FROM user_events e
    JOIN {{ ref('silver_user_sequences') }} s ON e.user_id = s.user_id
    WHERE e.event_rank >= 2
),

-- Lấy prefix history: slice từ đầu đến (target_rank - 1), tối đa max_len - 1 phần tử
sliding_windows AS (
    SELECT
        user_id,
        target_item,
        target_rank,
        total_events,
        target_timestamp,
        -- Lấy (target_rank - 1) items ngay TRƯỚC target_item
        -- Bắt đầu từ vị trí: GREATEST(1, target_rank - (max_len - 1))
        -- Lấy số lượng: LEAST(target_rank - 1, max_len - 1)
        SLICE(
            full_sequence,
            GREATEST(1, CAST(target_rank - ({{ max_len }} - 1) AS INT)),
            LEAST(CAST(target_rank - 1 AS INT), {{ max_len }} - 1)
        ) AS s_item_full
    FROM events_with_seq
),

split_assigned AS (
    SELECT
        sw.*,
        LEAST(SIZE(sw.s_item_full), {{ max_len }} - 1) AS s_item_len,
        CASE
            WHEN (sw.target_rank * 1.0 / sw.total_events) <= 0.8 THEN 'train'
            WHEN (sw.target_rank * 1.0 / sw.total_events) <= 0.9 THEN 'val'
            ELSE 'test'
        END AS split
    FROM sliding_windows sw
)

SELECT
    HASH(user_id, target_rank)  AS sample_id,
    user_id,
    s_item_full                 AS s_item,
    s_item_len,
    target_item                 AS item,
    split,
    target_timestamp            AS timestamp
FROM split_assigned
ORDER BY split, user_id, target_rank