-- Gold item features: SERVE TABLE duy nhất cho inference + ANN search
-- JOIN silver_items (metadata + popularity signals) + silver_visual_embeddings (768-dim)
-- Bảng này là "nguồn sự thật" về item cho toàn bộ recommendation pipeline
SELECT
    si.item_id,
    si.title,
    si.like_count,
    si.view_count,
    si.like_log,
    si.view_log,
    sve.visual_embedding,
    sve.embedding_dim,
    current_timestamp()     AS updated_at
FROM {{ ref('silver_items') }} si
JOIN {{ ref('silver_visual_embeddings') }} sve
    ON si.item_id = sve.item_id
