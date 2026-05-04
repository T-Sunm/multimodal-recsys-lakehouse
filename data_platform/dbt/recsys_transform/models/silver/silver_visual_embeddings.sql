-- Silver visual embeddings: validate 768-dim, lọc invalid
-- Nguồn: bronze_visual_embeddings (đọc từ Parquet đã convert từ .pt)
SELECT
    CAST(item_id AS INT)                                AS item_id,
    visual_embedding,                                   -- ARRAY<FLOAT> 768-dim
    SIZE(visual_embedding)                              AS embedding_dim,
    ingested_at
FROM {{ ref('bronze_visual_embeddings') }}
WHERE item_id IS NOT NULL
  AND visual_embedding IS NOT NULL
  AND SIZE(visual_embedding) = 768
