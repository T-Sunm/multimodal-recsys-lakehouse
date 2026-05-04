import torch
import pandas as pd

PT_PATH      = "microlens-5k/visual_embeddings.pt"
PARQUET_PATH = "microlens-5k/visual_embeddings.parquet"

print("Loading .pt ...")
visual_embeddings = torch.load(PT_PATH, map_location="cpu", weights_only=False)

print(f"Loaded {len(visual_embeddings)} items, converting ...")
rows = [
    {
        "item_id":          int(item_id),
        "visual_embedding": tensor.numpy().astype("float32").tolist(),
    }
    for item_id, tensor in visual_embeddings.items()
]

df = pd.DataFrame(rows)
df.to_parquet(PARQUET_PATH, index=False, engine="pyarrow", compression="snappy")
print(f"Done! Saved {len(df)} rows → {PARQUET_PATH}")
print(df.head(3))