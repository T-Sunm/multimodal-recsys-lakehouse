-- Bronze visual embeddings: passthrough từ Parquet đã được export
-- bởi utils/convert_visual_embeddings_to_parquet.py (Swin 768-dim)
SELECT
    item_id,
    visual_embedding,       -- ARRAY<FLOAT> 768-dim
    current_timestamp()     AS ingested_at
FROM {{ source('recsys_raw', 'visual_embeddings_staging') }}
