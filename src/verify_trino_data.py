import os
import torch
import pandas as pd
from trino_service import fetch_training_samples, fetch_visual_embeddings
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình đường dẫn lưu trữ
DATA_DIR = r"E:\AIO\Project\multimodal-recsys-lakehouse\data"
SAMPLES_PATH = os.path.join(DATA_DIR, "verify_samples.csv")
EMBEDDINGS_PATH = os.path.join(DATA_DIR, "verify_embeddings.pt")

def main():
    # 1. Tạo thư mục data nếu chưa có
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        logger.info(f"Created directory: {DATA_DIR}")

    try:
        # 2. Kiểm tra fetch training samples
        logger.info("--- Step 1: Fetching Training Samples ---")
        df = fetch_training_samples()
        
        # In thông tin kiểm tra
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Split distribution:\n{df['split'].value_counts()}")
        
        # Lưu ra CSV để xem thử
        df.to_csv(SAMPLES_PATH, index=False)
        logger.info(f"Saved samples to: {SAMPLES_PATH}")

        # 3. Kiểm tra fetch visual embeddings
        logger.info("\n--- Step 2: Fetching Visual Embeddings ---")
        
        # Để fetch được embeddings, ta cần một map item_id (vì trino_service yêu cầu)
        # Ở đây ta lấy tất cả item_id xuất hiện trong gold_df để test
        unique_items = df['item'].unique()
        test_item_map = {iid: iid for iid in unique_items} # map 1-1 để lấy raw data
        
        embeddings = fetch_visual_embeddings(test_item_map)
        
        if embeddings:
            logger.info(f"Fetched {len(embeddings)} embeddings.")
            first_key = list(embeddings.keys())[0]
            logger.info(f"Sample Embedding Shape (ID {first_key}): {embeddings[first_key].shape}")
            
            # Lưu ra file .pt
            torch.save(embeddings, EMBEDDINGS_PATH)
            logger.info(f"Saved embeddings to: {EMBEDDINGS_PATH}")
        else:
            logger.warning("No embeddings found!")

        logger.info("\n✅ Verification Completed Successfully!")

    except Exception as e:
        logger.error(f"❌ Verification Failed: {str(e)}")

if __name__ == "__main__":
    main()
