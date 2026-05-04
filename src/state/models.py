"""PyTorch Model loading and caching.

Loads NeuMF and SeqNeuMF models dynamically from the checkpoints/ directory.
Uses Streamlit caching to keep models and large tensors in memory.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
import streamlit as st
import torch

from src.neumf import NeuMF
from src.seqneumf import SeqNeuMF

_ROOT = Path(__file__).parent.parent.parent
_CHECKPOINTS_DIR = _ROOT / "checkpoints"
_DATA_DIR = _ROOT / "data" / "microlens-5k"


# ── Finding Checkpoints ───────────────────────────────────────────────────────

def _get_latest_checkpoint(prefix: str) -> Path | None:
    """Find the checkpoint with the highest epoch number matching the prefix."""
    if not _CHECKPOINTS_DIR.exists():
        return None

    best_file = None
    max_epoch = -1

    for f in _CHECKPOINTS_DIR.glob(f"{prefix}_Epoch*.model"):
        # e.g., neumf_factor8neg4_Epoch19_HR0.3783_NDCG0.2205.model
        match = re.search(r"_Epoch(\d+)_", f.name)
        if match:
            epoch = int(match.group(1))
            if epoch > max_epoch:
                max_epoch = epoch
                best_file = f

    return best_file


# ── Loading Mappings & Embeddings ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def get_id_mappings() -> tuple[dict[str, int], dict[str, int], list[int]]:
    """Return (user_to_internal, item_to_internal, all_internal_item_ids)."""
    pairs_path = _DATA_DIR / "pairs.csv"
    if not pairs_path.exists():
        return {}, {}, []

    df = pd.read_csv(pairs_path, dtype={"user": str, "item": str})

    users = df["user"].drop_duplicates().reset_index(drop=True)
    user_to_internal = {u: i for i, u in enumerate(users)}

    items = df["item"].drop_duplicates().reset_index(drop=True)
    item_to_internal = {itm: i for i, itm in enumerate(items)}
    all_internal_items = list(item_to_internal.values())

    return user_to_internal, item_to_internal, all_internal_items


@st.cache_resource(show_spinner=False)
def get_visual_embeddings() -> dict[int, torch.Tensor]:
    """Load visual embeddings mapped to internal item IDs."""
    emb_path = _DATA_DIR / "visual_embeddings.pt"
    if not emb_path.exists():
        return {}

    _, item_to_internal, _ = get_id_mappings()
    raw_visual = torch.load(str(emb_path), weights_only=False)

    return {
        item_to_internal[str(k)]: v
        for k, v in raw_visual.items()
        if str(k) in item_to_internal
    }


# ── Loading Models ────────────────────────────────────────────────────────────

def _build_config(num_users: int, num_items: int, is_seq: bool = False) -> dict:
    config = {
        "num_users": num_users,
        "num_items": num_items,
        "latent_dim_mf": 8,
        "latent_dim_mlp": 8,
        "layers": [16, 64, 32, 16, 8],
        "visual_dim": 768,
        "weight_init_gaussian": False,
    }
    if is_seq:
        config.update({
            "use_seq_user": True,
            "maxlen": 50,
            "seq_hidden_units": 50,
            "num_heads": 1,
            "num_blocks": 2,
            "dropout_rate": 0.0,
        })
    return config


@st.cache_resource(show_spinner="Loading PyTorch models…")
def get_models() -> dict[str, torch.nn.Module]:
    """Load the latest NeuMF and SeqNeuMF models into memory."""
    user_to_internal, item_to_internal, _ = get_id_mappings()
    num_users = len(user_to_internal)
    num_items = len(item_to_internal)

    if num_users == 0 or num_items == 0:
        return {}

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    models_dict = {}

    # Load NeuMF
    ckpt_neumf = _get_latest_checkpoint("neumf_factor8neg4")
    if ckpt_neumf:
        model = NeuMF(_build_config(num_users, num_items, is_seq=False))
        model.load_state_dict(torch.load(ckpt_neumf, map_location=device, weights_only=True))
        model.to(device)
        model.eval()
        models_dict["NeuMF"] = model

    # Load SeqNeuMF
    ckpt_seq = _get_latest_checkpoint("seqneumf_factor8neg4")
    if ckpt_seq:
        model = SeqNeuMF(_build_config(num_users, num_items, is_seq=True))
        model.load_state_dict(torch.load(ckpt_seq, map_location=device, weights_only=True))
        model.to(device)
        model.eval()
        models_dict["SeqNeuMF"] = model

    return models_dict
