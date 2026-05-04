"""Real PyTorch MicroLens Recommender.

Supports:
- Tag Overlap (Baseline fallback)
- NeuMF (Collaborative Filtering + Visual)
- SeqNeuMF (Sequential + CF + Visual)
"""
from __future__ import annotations

import torch

from src.state.models import get_models, get_id_mappings, get_visual_embeddings


# ── Types ─────────────────────────────────────────────────────────────────────

RecommendationResult = dict


# ── PyTorch Inference ─────────────────────────────────────────────────────────

def _predict_pytorch(
    model_name: str,
    user_id: str,
    user_history_ids: list[str],
    candidates: list[dict],
) -> list[tuple[float, dict, list[str], list[str]]]:
    """Score candidates using the real PyTorch model."""
    models = get_models()
    model = models.get(model_name)

    # Fallback if model not loaded
    if not model:
        return []

    user_map, item_map, _ = get_id_mappings()
    visual_embs = get_visual_embeddings()

    internal_uid = user_map.get(user_id)
    if internal_uid is None:
        return []

    device = next(model.parameters()).device
    scored = []

    # Prepare inputs
    internal_cands = []
    valid_cands = []
    for cand in candidates:
        iid = cand.get("id")
        internal_iid = item_map.get(str(iid))
        if internal_iid is not None:
            internal_cands.append(internal_iid)
            valid_cands.append(cand)

    if not valid_cands:
        return []

    user_tensor = torch.tensor([internal_uid] * len(internal_cands), dtype=torch.long).to(device)
    item_tensor = torch.tensor(internal_cands, dtype=torch.long).to(device)

    # Visual features
    visual_tensors = []
    default_visual = torch.zeros(768)
    for i in internal_cands:
        visual_tensors.append(visual_embs.get(i, default_visual))
    visual_batch = torch.stack(visual_tensors).to(device)

    # Sequence for SeqNeuMF
    if model_name == "SeqNeuMF":
        # Convert string history to internal IDs
        hist = [item_map.get(str(i)) for i in user_history_ids]
        hist = [i for i in hist if i is not None]

        # Padding to maxlen 50 (from inference config)
        maxlen = 50
        seq = hist[-maxlen:]
        padded_seq = [0] * (maxlen - len(seq)) + seq
        seq_tensor = torch.tensor([padded_seq] * len(internal_cands), dtype=torch.long).to(device)

    with torch.no_grad():
        if model_name == "SeqNeuMF":
            scores = model(user_tensor, seq_tensor, item_tensor, visual_batch)
        else:
            scores = model(user_tensor, item_tensor, visual_batch)
        scores = scores.squeeze().cpu().numpy()

    # Ensure scores is iterable (handle batch_size=1)
    if scores.ndim == 0:
        scores = [float(scores)]
    else:
        scores = scores.tolist()

    # Format output
    for score, cand in zip(scores, valid_cands):
        # We don't have distinct reason tags for black-box NN, so we provide an explanation.
        # Scale score artificially to [0,1] for display purposes if it's raw logits.
        # NeuMF usually outputs sigmoid [0,1], so we just round it.
        display_score = round(float(score), 4)
        scored.append((display_score, cand, [f"{model_name} Prediction"], []))

    return scored


# ── Public API ────────────────────────────────────────────────────────────────

def recommend(
    user_id: str | None,
    user_history: list[dict],
    all_items: list[dict],
    top_k: int = 6,
    model_type: str = "SeqNeuMF",
) -> list[RecommendationResult]:
    """Return top-k recommendations for the user.

    If model_type is a PyTorch model and user_id is valid, runs real AI inference.
    """
    if not user_id:
        return []

    seen_ids = {str(item.get("id")) for item in user_history}
    candidates = [item for item in all_items if str(item.get("id")) not in seen_ids]

    scored = []
    if model_type in ["NeuMF", "SeqNeuMF"]:
        history_ids = [str(item.get("id")) for item in user_history[::-1]] # Oldest to newest
        scored = _predict_pytorch(model_type, user_id, history_ids, candidates)

    if not scored:
        # Fallback to random if something fails
        import random
        for cand in candidates:
            scored.append((random.random(), cand, ["Fallback"], []))

    # Sort descending
    scored.sort(key=lambda x: x[0], reverse=True)

    results: list[RecommendationResult] = []
    for rank, (score, item, reason_tags, source_seed_ids) in enumerate(
        scored[:top_k], start=1
    ):
        results.append(
            {
                "item_id": str(item.get("id", "")),
                "score": score,
                "rank": rank,
                "reason_tags": reason_tags,
                "source_seed_ids": source_seed_ids,
            }
        )

    return results
