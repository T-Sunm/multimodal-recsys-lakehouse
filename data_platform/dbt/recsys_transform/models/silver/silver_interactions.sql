-- Silver interactions: cast types, deduplicate, parse timestamp
-- Dataset MicroLens-5k là static/curated academic data nên không cần anti-bot
-- Timestamp luôn là unix ms (13 chữ số) → chia 1000 để ra unix seconds
WITH raw AS (
    SELECT
        CAST(raw_user         AS INT)                        AS user_id,
        CAST(raw_item         AS INT)                        AS item_id,
        CAST(raw_timestamp_ms AS BIGINT) / 1000              AS timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY raw_user, raw_item, raw_timestamp_ms
            ORDER BY ingested_at DESC
        )                                                    AS rn
    FROM {{ ref('bronze_interactions') }}
    WHERE raw_user IS NOT NULL
      AND raw_item IS NOT NULL
      AND raw_timestamp_ms IS NOT NULL
)

SELECT
    user_id,
    item_id,
    timestamp,
    CAST(FROM_UNIXTIME(timestamp, 'yyyy-MM-dd') AS DATE) AS event_date
FROM raw
WHERE rn = 1
  AND user_id > 0
  AND item_id > 0
