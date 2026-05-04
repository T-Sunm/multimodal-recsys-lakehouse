"""Explore page – browse real MicroLens-5k items.

Layout:
  Top Bar → User selector + inline Search + Sort + Filter controls
  Center  → Item grid (videos first, paginated, 4 cols)
  Right   → User Watch History
"""
from __future__ import annotations

import streamlit as st

try:
    import streamlit_shadcn_ui as ui
    _HAS_SCU = True
except ImportError:
    _HAS_SCU = False

from src.data.loader import load_items, get_all_user_ids, get_user_history, get_default_video_user
from src.components.media_card import render_media_card, render_watch_history
from src.state.session import (
    init_session,
    get_selected_user, set_selected_user,
    get_merged_history,
)

_GRID_COLS   = 4
_PAGE_SIZE   = 16
_SORT_OPTIONS = {
    "🔥 Most Viewed": "views",
    "❤️ Most Liked":  "likes",
    "🔢 ID (asc)":    "id",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sort_items(items: list[dict], key: str, videos_first: bool = True) -> list[dict]:
    """Sort items by key; optionally float items with a video file to the top."""
    def _sort_key(x):
        has_no_video = 0 if (videos_first and x.get("video")) else 1
        val = x.get(key)
        return (has_no_video, val is None, -(val or 0))
    return sorted(items, key=_sort_key)


def _filter_items(items: list[dict], query: str, only_video: bool = False) -> list[dict]:
    if only_video:
        items = [i for i in items if i.get("video")]
    if not query:
        return items
    q = query.strip().lower()
    return [i for i in items if q in str(i.get("title", "")).lower()
            or q in str(i.get("id", "")).lower()]


# ── Page ──────────────────────────────────────────────────────────────────────

def render() -> None:
    init_session()

    # ── Auto-select default user on first load ────────────────────────────
    current_user = get_selected_user()
    if not current_user and not st.session_state.get("ml_user_default_set"):
        def_user = get_default_video_user()
        if def_user:
            current_user = def_user
            set_selected_user(current_user)
        st.session_state["ml_user_default_set"] = True

    all_users = get_all_user_ids()
    user_options = ["— None —"] + all_users
    current_idx = user_options.index(current_user) if current_user in user_options else 0

    # ── Top control bar (title row + controls row) ────────────────────────
    title_col, spacer = st.columns([2, 3])
    with title_col:
        st.title("🔍 Explore")

    # Control row: User | Search | Sort | Video-only toggle
    u_col, s_col, sort_col, tog_col = st.columns([1.6, 2, 1.6, 1], gap="small")

    with u_col:
        selected = st.selectbox(
            "👤 User",
            user_options,
            index=current_idx,
            help="Select a user to view their history and run personalized recommendations.",
            label_visibility="visible",
        )
        if selected == "— None —":
            if current_user is not None:
                set_selected_user(None)
                st.rerun()
        elif selected != current_user:
            set_selected_user(selected)
            st.rerun()

    with s_col:
        query = st.text_input(
            "🔎 Search",
            placeholder="Search by title or ID…",
            label_visibility="visible",
        )

    with sort_col:
        sort_label = st.selectbox(
            "↕ Sort by",
            list(_SORT_OPTIONS.keys()),
            label_visibility="visible",
        )

    with tog_col:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        videos_first = st.toggle("🎬 Videos First", value=True, help="Push items with a playable video to the top")

    st.divider()

    # ── Main Content Area ─────────────────────────────────────────────────
    main_col, right_col = st.columns([4, 1.5], gap="large")

    # ── Left: paginated grid ──────────────────────────────────────────────
    with main_col:
        with st.spinner("Loading items…"):
            all_items = load_items()

        filtered = _filter_items(all_items, query, only_video=False)
        sorted_items = _sort_items(filtered, _SORT_OPTIONS[sort_label], videos_first=videos_first)

        if not sorted_items:
            st.info("No items match your search.", icon="🔎")
            return

        total = len(sorted_items)
        total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)

        page_col, info_col = st.columns([2, 3])
        with page_col:
            page = st.number_input(
                "Page", min_value=1, max_value=total_pages,
                value=1, step=1, key="ml_page",
                label_visibility="collapsed",
            )
        with info_col:
            start = (page - 1) * _PAGE_SIZE + 1
            end = min(page * _PAGE_SIZE, total)
            video_count = sum(1 for i in sorted_items if i.get("video"))
            st.caption(
                f"Showing **{start}–{end}** of **{total}** items"
                f" · 🎬 **{video_count}** total videos"
            )

        page_items = sorted_items[(page - 1) * _PAGE_SIZE: page * _PAGE_SIZE]
        rows = [page_items[i: i + _GRID_COLS] for i in range(0, len(page_items), _GRID_COLS)]

        for row in rows:
            cols = st.columns(_GRID_COLS, gap="small")
            for col, item in zip(cols, row):
                with col:
                    render_media_card(item, key_prefix="explore")

    # ── Right: watch history ──────────────────────────────────────────────
    with right_col:
        st.subheader("📋 Watch History")

        if current_user:
            base_history = get_user_history(current_user)
            history = get_merged_history(base_history)

            if _HAS_SCU:
                if ui.button("🚀 Run Recommendations", key="go_rec", variant="default", class_name="w-full mb-4"):
                    st.session_state["nav_selection"] = "⭐ Recommend"
                    st.rerun()
            else:
                if st.button("🚀 Run Recommendations", key="go_rec", use_container_width=True):
                    st.session_state["nav_selection"] = "⭐ Recommend"
                    st.rerun()

            render_watch_history(history, current_user)
        else:
            st.info("Select a user from the dropdown above to view their history.", icon="👈")

