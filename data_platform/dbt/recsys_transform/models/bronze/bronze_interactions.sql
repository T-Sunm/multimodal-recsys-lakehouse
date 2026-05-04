-- Bronze interactions: raw event log từ pairs.csv
-- Chỉ rename columns + thêm ingestion metadata, không transform
SELECT
    user                    AS raw_user,
    item                    AS raw_item,
    timestamp               AS raw_timestamp_ms,
    current_timestamp()     AS ingested_at
FROM {{ source('recsys_raw', 'interactions_staging') }}
