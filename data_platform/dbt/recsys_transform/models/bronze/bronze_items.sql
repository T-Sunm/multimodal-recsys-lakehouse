-- Bronze items: JOIN titles.csv + likes_and_views.txt
-- Merge metadata thành 1 staging view trước khi clean ở Silver
SELECT
    t.item                  AS raw_item_id,
    t.title                 AS raw_title,
    lv.like_count           AS raw_like_count,
    lv.view_count           AS raw_view_count,
    current_timestamp()     AS ingested_at
FROM {{ source('recsys_raw', 'items_staging') }} t
LEFT JOIN {{ source('recsys_raw', 'likes_views_staging') }} lv
    ON t.item = lv.item
