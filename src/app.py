"""MicroLens Recommender Workbench – Streamlit entrypoint.

Launch with:
    streamlit run src/app.py

Architecture:
    src/app.py              ← this file (navigation shell only)
    src/pages/explore.py   ← Explore screen
    src/components/         ← reusable UI components
    src/state/session.py    ← session state API
    src/data/               ← data loading / mock data
    src/services/           ← recommender logic (future)
"""

from __future__ import annotations

# ── sys.path fix ──────────────────────────────────────────────────────────────
# Streamlit adds the script's directory (src/) to sys.path, not the project
# root.  Insert the root so that `from src.xxx` resolves correctly.
import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import streamlit as st

from src.state.session import init_session

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MicroLens Workbench",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global styles ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Hide default Streamlit sidebar navigation & branding */
    [data-testid="stSidebarNav"] { display: none; }
    #MainMenu, footer { visibility: hidden; }
    header[data-testid="stHeader"] { background: transparent; }

    html, body, [class*="css"] { font-family: "Inter", sans-serif; }

    /* ── Sidebar shell ── */
    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 1.5rem;
        display: flex;
        flex-direction: column;
        gap: 0;
    }

    /* ── Nav buttons ── */
    section[data-testid="stSidebar"] .stButton > button {
        width: 100%;
        border: none !important;
        background: transparent !important;
        box-shadow: none !important;
        padding: 9px 14px;
        border-radius: 8px;
        font-size: 14px;
        font-weight: 500;
        color: inherit;
        transition: background 0.15s ease, transform 0.15s ease;
        margin-bottom: 2px;
    }
    /* Force left-alignment for Streamlit buttons */
    section[data-testid="stSidebar"] .stButton > button > div {
        justify-content: flex-start !important;
        width: 100%;
    }
    section[data-testid="stSidebar"] .stButton > button p {
        text-align: left !important;
        margin: 0 !important;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(99, 102, 241, 0.12) !important;
        transform: translateX(3px);
    }
    /* Active nav item */
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
        background: rgba(99, 102, 241, 0.18) !important;
        color: #818cf8 !important;
        font-weight: 600;
    }

    /* ── Sidebar info card ── */
    .sidebar-info-card {
        background: rgba(99, 102, 241, 0.08);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 10px;
        padding: 10px 14px;
        margin-top: 4px;
    }
    .sidebar-info-card .label {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: .06em;
        text-transform: uppercase;
        color: #6366f1;
        margin-bottom: 2px;
    }
    .sidebar-info-card .value {
        font-size: 14px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Session init ──────────────────────────────────────────────────────────────
init_session()

# ── Navigation ────────────────────────────────────────────────────────────────
pages = {
    "🔍 Explore": "src/pages/explore.py",
    "⭐ Recommend": "src/pages/recommend.py",
    "📊 Compare Models": "src/pages/compare.py",
    "🔬 Inspect Item": "src/pages/inspect.py",
}

with st.sidebar:
    # ── Brand ────────────────────────────────────────────────────────────
    st.markdown(
        '<div style="display:flex;align-items:center;gap:10px;padding:0 4px 4px;">'
        '<span style="font-size:26px;">🎬</span>'
        '<div><p style="margin:0;font-size:18px;font-weight:700;line-height:1.1;">MicroLens</p>'
        '<p style="margin:0;font-size:11px;color:#94a3b8;letter-spacing:.05em;">REC WORKBENCH</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    # ── Navigation ───────────────────────────────────────────────────────
    _PAGES = [
        ("🔍", "Explore"),
        ("⭐", "Recommend"),
    ]
    _PAGE_KEYS = [f"{icon} {label}" for icon, label in _PAGES]
    if "nav_selection" not in st.session_state:
        st.session_state["nav_selection"] = _PAGE_KEYS[0]

    for (icon, label), key in zip(_PAGES, _PAGE_KEYS):
        _active = st.session_state["nav_selection"] == key
        _btn_type = "primary" if _active else "secondary"
        if st.button(
            f"{icon}  {label}",
            key=f"nav_{key}",
            use_container_width=True,
            type=_btn_type,
        ):
            st.session_state["nav_selection"] = key
            st.rerun()

    st.divider()

    # ── Context info ─────────────────────────────────────────────────────
    from src.state.session import seed_count, get_selected_user
    n = seed_count()
    uid = get_selected_user()

    st.markdown(
        f'<div class="sidebar-info-card">'
        f'<div class="label">Active User</div>'
        f'<div class="value">👤 {uid if uid else "— None —"}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="sidebar-info-card" style="margin-top:8px;">'
        f'<div class="label">Seeds Selected</div>'
        f'<div class="value">🌱 {n}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if n > 0 and st.session_state["nav_selection"] != "⭐ Recommend":
        st.markdown("<div style='margin-top:10px;'></div>", unsafe_allow_html=True)
        if st.button("🚀 Run Recommendations", key="sidebar_run", use_container_width=True):
            st.session_state["nav_selection"] = "⭐ Recommend"
            st.rerun()

selection = st.session_state.get("nav_selection", "🔍 Explore")

# ── Page dispatch ─────────────────────────────────────────────────────────────
if selection == "🔍 Explore":
    from src.pages.explore import render
    render()

elif selection == "⭐ Recommend":
    from src.pages.recommend import render as render_recommend
    render_recommend()
