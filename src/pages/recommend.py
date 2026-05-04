"""Recommend page – run real AI recommendations for selected user.

Layout (3-zone, desktop):
  left   → run controls (top-k, model selector)
  center → result card grid (3 cols)
  right  → current user history summary
"""

from __future__ import annotations

import streamlit as st

try:
    import streamlit_shadcn_ui as ui
    _HAS_SCU = True
except ImportError:
    _HAS_SCU = False

from src.data.loader import load_items, get_user_history
from src.services.recommender import recommend
from src.components.media_card import render_result_card, render_watch_history
from src.state.session import init_session, get_selected_user, seed_count, get_merged_history

_GRID_COLS = 3
_DEFAULT_TOP_K = 6
_RUN_KEY = "rec_results"


# ── Page render ───────────────────────────────────────────────────────────────

def render() -> None:
    init_session()

    st.title("⭐ Recommend")
    st.caption("Generate AI recommendations based on user history.")

    user_id = get_selected_user()

    # ── Empty state: no user ─────────────────────────────────────────────
    if not user_id:
        st.info(
            "**No User selected.**  "
            "Go to the [Explore page](#) and select a User ID to run predictions.",
            icon="👤",
        )
        if _HAS_SCU:
            if ui.button("← Back to Explore", key="back_explore", variant="default"):
                st.session_state["nav_selection"] = "🔍 Explore"
                st.rerun()
        else:
            if st.button("← Back to Explore", key="back_explore"):
                st.session_state["nav_selection"] = "🔍 Explore"
                st.rerun()
        return

    base_history = get_user_history(user_id)
    history = get_merged_history(base_history)  # seeds prepended as "just viewed"

    # ── Three-zone layout ─────────────────────────────────────────────────
    left, center, right = st.columns([1, 4, 1.4], gap="medium")

    # ── Left: controls ────────────────────────────────────────────────────
    with left:
        st.subheader("Settings")

        model_type = st.radio(
            "Model Architecture",
            ["SeqNeuMF", "NeuMF"],
            help="SeqNeuMF considers user sequence history. NeuMF only uses matrix factorization.",
        )

        top_k = st.slider(
            "Top-k results",
            min_value=2,
            max_value=12,
            value=_DEFAULT_TOP_K,
            step=1,
            key="rec_top_k",
        )

        st.divider()

        # Run button
        run_clicked = False
        if _HAS_SCU:
            run_clicked = ui.button(
                "🚀 Predict",
                key="rec_run_btn",
                variant="default",
                class_name="w-full",
            )
        else:
            run_clicked = st.button(
                "🚀 Predict",
                key="rec_run_btn",
                use_container_width=True,
            )

        if run_clicked:
            with st.spinner(f"Running {model_type} inference..."):
                results = recommend(
                    user_id=user_id,
                    user_history=history,
                    all_items=load_items(),
                    top_k=top_k,
                    model_type=model_type,
                )
            st.session_state[_RUN_KEY] = results
            # Clear hide flags
            for key in list(st.session_state.keys()):
                if key.startswith("rec_hide_"):
                    del st.session_state[key]
            st.rerun()

        if st.session_state.get(_RUN_KEY):
            st.divider()
            if _HAS_SCU:
                if ui.button("✕ Clear results", key="rec_clear", variant="ghost", class_name="w-full text-xs"):
                    st.session_state[_RUN_KEY] = None
                    st.rerun()
            else:
                if st.button("✕ Clear results", key="rec_clear", use_container_width=True):
                    st.session_state[_RUN_KEY] = None
                    st.rerun()

    # ── Right: User History ───────────────────────────────────────────────
    with right:
        st.subheader("📋 Context")
        render_watch_history(history, user_id, max_items=15, height=580)


    # ── Center: results grid ──────────────────────────────────────────────
    with center:
        results: list[dict] | None = st.session_state.get(_RUN_KEY)

        if results is None:
            st.info("Click **🚀 Predict** in the left panel to run the PyTorch model.", icon="🧠")
            return

        if not results:
            st.warning("No recommendations returned from the model.", icon="⚠️")
            return

        all_items = load_items()
        item_by_id = {str(it["id"]): it for it in all_items}

        visible = [r for r in results if not st.session_state.get(f"rec_hide_{r['item_id']}")]

        if not visible:
            st.info("All results hidden. Clear results to reset.")
            return

        st.caption(f"Showing **{len(visible)}** predictions via **{model_type}**")

        rows = [visible[i : i + _GRID_COLS] for i in range(0, len(visible), _GRID_COLS)]
        for row in rows:
            cols = st.columns(_GRID_COLS, gap="small")
            for col, result in zip(cols, row):
                item = item_by_id.get(str(result["item_id"]))
                if item is None:
                    continue
                with col:
                    render_result_card(item, result, key_prefix="rec")
