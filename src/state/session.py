"""Centralized session state management for MicroLens Recommender Workbench."""

from __future__ import annotations

import streamlit as st


# ── Keys ─────────────────────────────────────────────────────────────────────
_SEEDS_KEY        = "ml_seeds"           # dict[str, dict]  {item_id -> item}
_EXTRA_HIST_KEY   = "ml_extra_history"   # list[dict]  seeds pushed to top of history
_SEARCH_KEY       = "ml_search_query"
_SORT_KEY         = "ml_sort_order"
_USER_KEY         = "ml_selected_user"


def init_session() -> None:
    """Initialise all session-state keys with safe defaults.

    Call once at the top of every page so that every key is guaranteed
    to exist before any widget reads it.
    """
    defaults: dict[str, object] = {
        _SEEDS_KEY:      {},
        _EXTRA_HIST_KEY: [],
        _SEARCH_KEY:     "",
        _SORT_KEY:       "views",
        _USER_KEY:       None,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


# ── Seeds API ─────────────────────────────────────────────────────────────────

def add_seed(item: dict) -> None:
    """Add *item* to the seed basket AND prepend it to the session history."""
    item_id = str(item["id"])
    st.session_state[_SEEDS_KEY][item_id] = item

    # Prepend to extra history (deduped: remove any previous occurrence first)
    extra: list[dict] = st.session_state[_EXTRA_HIST_KEY]
    extra = [h for h in extra if str(h.get("id")) != item_id]
    extra.insert(0, item)
    st.session_state[_EXTRA_HIST_KEY] = extra


def remove_seed(item_id: str | int) -> None:
    """Remove *item_id* from the seed basket AND from the extra history list."""
    iid = str(item_id)
    st.session_state[_SEEDS_KEY].pop(iid, None)
    # Also remove from extra history so it drops back to its original position
    st.session_state[_EXTRA_HIST_KEY] = [
        h for h in st.session_state[_EXTRA_HIST_KEY]
        if str(h.get("id")) != iid
    ]


def is_seed(item_id: str | int) -> bool:
    """Return True if *item_id* is currently in the seed basket."""
    return str(item_id) in st.session_state[_SEEDS_KEY]


def get_seeds() -> dict[str, dict]:
    """Return a snapshot of the current seed basket."""
    return dict(st.session_state[_SEEDS_KEY])


def seed_count() -> int:
    return len(st.session_state[_SEEDS_KEY])


def get_merged_history(base_history: list[dict]) -> list[dict]:
    """Return extra seeded items (newest first) prepended to base_history (deduped).

    Seeds that were "used" appear at the top as if they were just viewed.
    Original history items that duplicate a seed are suppressed.
    """
    extra: list[dict] = list(st.session_state.get(_EXTRA_HIST_KEY, []))
    extra_ids = {str(h.get("id")) for h in extra}
    deduped_base = [h for h in base_history if str(h.get("id")) not in extra_ids]
    return extra + deduped_base


# ── Selected user API ─────────────────────────────────────────────────────────

def set_selected_user(user_id: str | None) -> None:
    st.session_state[_USER_KEY] = user_id


def get_selected_user() -> str | None:
    return st.session_state.get(_USER_KEY)
