import logging

import torch
import pandas as pd
from trino.dbapi import connect
from trino.exceptions import DatabaseError, OperationalError

from config import TRINO_HOST, TRINO_PORT, TRINO_USER

logger = logging.getLogger(__name__)


class TrinoServiceError(Exception):
    pass


def _get_connection():
    return connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)


def _run_query(query: str) -> pd.DataFrame:
    try:
        with _get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
    except (DatabaseError, OperationalError) as e:
        logger.error("Trino query failed: %s", e)
        raise TrinoServiceError(str(e)) from e


def fetch_training_samples() -> pd.DataFrame:
    """
    Fetch pre-processed training samples from Gold layer.
    dbt has already handled: filtering, sequence building (s_item), and train/val/test split.
    Returns: DataFrame [user_id, s_item (list), item, split, timestamp]
    """
    df = _run_query("""
        SELECT user_id, s_item, item, split, timestamp
        FROM iceberg.recsys_gold.gold_training_samples
    """)
    logger.info("Loaded %d samples from Trino Gold Layer", len(df))
    return df


def fetch_visual_embeddings(orig_to_new: dict) -> dict:
    """
    Fetch visual embeddings from Gold layer.
    Args:
        orig_to_new: {raw_item_id (int) -> new_itemId (int)} built from reindex in train.py
    Returns: dict {new_itemId (int): torch.Tensor(768,)}
    """
    df = _run_query("""
        SELECT item_id, visual_embedding
        FROM iceberg.recsys_gold.gold_item_features
    """)
    result = {
        orig_to_new[row.item_id]: torch.tensor(row.visual_embedding, dtype=torch.float32)
        for row in df.itertuples(index=False)
        if row.item_id in orig_to_new
    }
    logger.info("Loaded %d visual embeddings from Trino", len(result))
    return result
