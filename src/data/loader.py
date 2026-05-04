"""Real data loader for MicroLens-5k dataset.

Files used:
  data/microlens-5k/titles.csv           item,title
  data/microlens-5k/likes_and_views.txt  item\\tlikes\\tviews  (tab, no header)
  data/microlens-5k/pairs.csv            user,item,timestamp
  data/microlens-5k/covers/{id}.jpg
  data/microlens-5k/videos/{id}.mp4
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

_ROOT = Path(__file__).parent.parent.parent
_DEFAULT_DIR = str(_ROOT / "data" / "microlens-5k")


# ── Raw CSV loaders (cached) ──────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _titles(data_dir: str) -> pd.DataFrame:
    return pd.read_csv(Path(data_dir) / "titles.csv", dtype={"item": str})


@st.cache_data(show_spinner=False)
def _lv(data_dir: str) -> pd.DataFrame:
    return pd.read_csv(
        Path(data_dir) / "likes_and_views.txt",
        sep="\t", header=None, names=["item", "likes", "views"],
        dtype={"item": str},
    )


@st.cache_data(show_spinner=False)
def _pairs(data_dir: str) -> pd.DataFrame:
    return pd.read_csv(
        Path(data_dir) / "pairs.csv",
        dtype={"user": str, "item": str},
    )


# ── Path helpers ──────────────────────────────────────────────────────────────

def _cover(item_id: str, d: Path) -> str | None:
    p = d / "covers" / f"{item_id}.jpg"
    return str(p) if p.exists() else None


def _video(item_id: str, d: Path) -> str | None:
    p = d / "videos" / f"{item_id}.mp4"
    return str(p) if p.exists() else None


def _make_item(row, d: Path) -> dict:
    iid = str(row.item)
    likes = getattr(row, "likes", None)
    views = getattr(row, "views", None)
    return {
        "id": iid,
        "title": str(row.title).strip() if pd.notna(row.title) else f"Item {iid}",
        "cover": _cover(iid, d),
        "preview_frames": [],          # dataset has no frame strips
        "video": _video(iid, d),
        "likes": int(likes) if pd.notna(likes) else None,
        "views": int(views) if pd.notna(views) else None,
        "tags": None,
        "meta": None,
    }


# ── Public API ────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_items(data_dir: str = _DEFAULT_DIR) -> list[dict]:
    """Return all catalogue items in canonical shape (cached)."""
    d = Path(data_dir)
    df = _titles(data_dir).merge(_lv(data_dir), on="item", how="left")
    return [_make_item(r, d) for r in df.itertuples()]


@st.cache_data(show_spinner=False)
def get_all_user_ids(data_dir: str = _DEFAULT_DIR) -> list[str]:
    """Return sorted list of unique user IDs."""
    return sorted(_pairs(data_dir)["user"].unique().tolist())


@st.cache_data(show_spinner=False)
def get_user_history(user_id: str, data_dir: str = _DEFAULT_DIR) -> list[dict]:
    """Items the user interacted with, most recent first."""
    d = Path(data_dir)
    pairs = _pairs(data_dir)
    user_pairs = (
        pairs[pairs["user"] == user_id]
        .sort_values("timestamp", ascending=False)
    )
    if user_pairs.empty:
        return []

    titles = _titles(data_dir)
    lv = _lv(data_dir)
    merged = user_pairs.merge(titles, on="item", how="left").merge(lv, on="item", how="left")
    return [_make_item(r, d) for r in merged.itertuples()]
@st.cache_data(show_spinner=False)
def get_default_video_user(data_dir: str = _DEFAULT_DIR) -> str | None:
    """Find the user who has interacted with the most videos (.mp4 files)."""
    d = Path(data_dir)
    videos_dir = d / "videos"
    if not videos_dir.exists():
        return None
    
    # Get all item IDs that have videos
    video_items = {f.stem for f in videos_dir.glob("*.mp4")}
    if not video_items:
        return None
        
    pairs = _pairs(data_dir)
    # Filter pairs where the item has a video
    video_interactions = pairs[pairs["item"].isin(video_items)]
    if video_interactions.empty:
        return None
        
    # Return user with most video interactions
    return video_interactions["user"].value_counts().index[0]
