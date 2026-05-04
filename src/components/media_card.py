"""Reusable MicroLens media card component.

Card layout (top → bottom):
  1. Cover image  (st.image — works for local paths & URLs; placeholder if None)
  2. Video player (▶ Play button toggles st.video inline; hidden if no video)
  3. Title        (bold, clamped to ~2 lines)
  4. Stats        (❤ likes  👁 views  — N/A when None)
  5. Actions      ("Use as seed" / "Remove seed" for explore cards)

No preview_frames strip — this dataset has no frame sequences.
Recommendation logic MUST NOT be placed here.
"""
from __future__ import annotations

import streamlit as st

try:
    import streamlit_shadcn_ui as ui
    _HAS_SCU = True
except ImportError:
    _HAS_SCU = False

from src.state.session import add_seed, remove_seed, is_seed

_PLACEHOLDER_H = 140   # px for no-cover placeholder


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(v: int | float | None) -> str:
    if v is None:
        return "N/A"
    n = int(v)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def _safe_id(item_id: object) -> str:
    return str(item_id)

import base64
def _render_cover(cover: str | None) -> None:
    if cover:
        try:
            with open(cover, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            ext = "png" if str(cover).endswith(".png") else "jpeg"
            st.markdown(
                f'<img src="data:image/{ext};base64,{b64}" '
                f'style="width:100%;height:160px;object-fit:cover;border-radius:4px;display:block;margin-bottom:8px;" />',
                unsafe_allow_html=True,
            )
        except Exception:
            st.image(cover, use_container_width=True)
    else:
        st.markdown(
            f'<div style="height:{_PLACEHOLDER_H}px;background:#1e293b;'
            f'border-radius:4px;display:flex;align-items:center;'
            f'justify-content:center;margin-bottom:8px;"><span style="color:#64748b;'
            f'font-size:13px;">No cover</span></div>',
            unsafe_allow_html=True,
        )


# ── Shared: Watch History sidebar ─────────────────────────────────────────────

def render_watch_history(
    history: list[dict],
    user_id: str,
    max_items: int = 20,
    height: int = 600,
) -> None:
    """Reusable watch-history panel (right column on Explore & Recommend pages).

    Items that are active seeds are shown with a 🌱 badge at the top.
    """
    from src.state.session import is_seed  # local import to avoid circular
    seed_count_in_hist = sum(1 for h in history if is_seed(str(h.get("id"))))
    caption = f"User `{user_id}` · **{len(history)}** interactions"
    if seed_count_in_hist:
        caption += f" · 🌱 **{seed_count_in_hist}** seeded"
    st.caption(caption)

    if not history:
        st.caption("No history found.")
        return

    with st.container(height=height, border=False):
        for h in history[:max_items]:
            item_id = str(h.get("id", ""))
            seeded = is_seed(item_id)

            hc, hi = st.columns([1.2, 2])
            with hc:
                cover = h.get("cover")
                if cover:
                    try:
                        with open(cover, "rb") as f:
                            b64 = base64.b64encode(f.read()).decode("utf-8")
                        ext = "png" if str(cover).endswith(".png") else "jpeg"
                        border = "2px solid #6366f1" if seeded else "none"
                        st.markdown(
                            f'<img src="data:image/{ext};base64,{b64}" '
                            f'style="width:100%;height:52px;object-fit:cover;'
                            f'border-radius:4px;display:block;border:{border};" />',
                            unsafe_allow_html=True,
                        )
                    except Exception:
                        st.image(cover, use_container_width=True)
                else:
                    st.markdown(
                        '<div style="width:100%;height:52px;'
                        'background:#1e293b;border-radius:4px;"></div>',
                        unsafe_allow_html=True,
                    )
            with hi:
                t = str(h.get("title", ""))[:48]
                seed_badge = '<span style="font-size:10px;background:#6366f1;color:#fff;border-radius:3px;padding:1px 5px;margin-right:4px;">SEED</span>' if seeded else ""
                video_icon = "🎥 " if h.get("video") else ""
                st.markdown(
                    f'<p style="font-size:12px;margin:0;line-height:1.35;'
                    f'padding-top:4px;">{seed_badge}{video_icon}{t}</p>',
                    unsafe_allow_html=True,
                )
            st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

        if len(history) > max_items:
            st.caption(f"…and {len(history) - max_items} more")



# ── Explore card ──────────────────────────────────────────────────────────────

def render_media_card(item: dict, key_prefix: str = "") -> None:
    """Render an explore-mode media card. Never crashes on missing fields."""
    try:
        _card_inner(item, key_prefix)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"⚠️ Card error (id={item.get('id','?')}): {exc}")


def _card_inner(item: dict, key_prefix: str) -> None:
    item_id = _safe_id(item.get("id", "unknown"))
    title   = str(item.get("title", "Untitled")).strip() or "Untitled"
    cover   = item.get("cover")
    video   = item.get("video")
    likes   = item.get("likes")
    views   = item.get("views")

    play_key = f"{key_prefix}_play_{item_id}"
    seed_key = f"{key_prefix}_seed_{item_id}"
    already_seed = is_seed(item_id)

    with st.container(border=True):
        # ── Cover ─────────────────────────────────────────────────────────
        _render_cover(cover)

        # ── Video player ───────────────────────────────────────────────────
        if video:
            if st.session_state.get(play_key):
                st.video(video)
                if st.button("✕ Close", key=f"{play_key}_close",
                             use_container_width=True):
                    st.session_state[play_key] = False
                    st.rerun()
            else:
                if st.button("▶ Play", key=f"{play_key}_open",
                             use_container_width=True):
                    st.session_state[play_key] = True
                    st.rerun()

        # ── Title ──────────────────────────────────────────────────────────
        st.markdown(
            f'<p style="font-weight:600;font-size:13px;line-height:1.35;'
            f'display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;'
            f'overflow:hidden;margin:4px 0 2px;">{title}</p>',
            unsafe_allow_html=True,
        )

        # ── Stats ──────────────────────────────────────────────────────────
        c1, c2 = st.columns(2)
        c1.caption(f"❤️ {_fmt(likes)}")
        c2.caption(f"👁 {_fmt(views)}")

        # ── Seed action ────────────────────────────────────────────────────
        if already_seed:
            btn_label, btn_variant = "✕ Remove seed", "destructive"
        else:
            btn_label, btn_variant = "＋ Use as seed", "default"

        if _HAS_SCU:
            if ui.button(btn_label, key=seed_key, variant=btn_variant,
                         class_name="w-full mt-1"):
                if already_seed:
                    remove_seed(item_id)
                else:
                    add_seed(item)
                st.rerun()
        else:
            if st.button(btn_label, key=seed_key, use_container_width=True):
                if already_seed:
                    remove_seed(item_id)
                else:
                    add_seed(item)
                st.rerun()


# ── Result card (Recommend page) ──────────────────────────────────────────────

def render_result_card(item: dict, result: dict, key_prefix: str = "rec") -> None:
    """Render a recommendation result card with rank, score, reason tags."""
    try:
        _result_inner(item, result, key_prefix)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"⚠️ Result card error (id={item.get('id','?')}): {exc}")


def _result_inner(item: dict, result: dict, key_prefix: str) -> None:
    item_id = _safe_id(item.get("id", "unknown"))
    title   = str(item.get("title", "Untitled")).strip() or "Untitled"
    cover   = item.get("cover")
    video   = item.get("video")
    likes   = item.get("likes")
    views   = item.get("views")

    rank        = result.get("rank", 0)
    score       = result.get("score", 0.0)
    reason_tags = result.get("reason_tags") or ["—"]

    hide_key = f"{key_prefix}_hide_{item_id}"
    play_key = f"{key_prefix}_play_{item_id}"

    if st.session_state.get(hide_key):
        return

    with st.container(border=True):
        # Rank + score row
        r1, r2 = st.columns([1, 1])
        r1.markdown(
            f'<span style="background:#7c3aed;color:#fff;border-radius:4px;'
            f'padding:1px 7px;font-size:11px;font-weight:700;">#{rank}</span>',
            unsafe_allow_html=True,
        )
        r2.markdown(
            f'<span style="background:#0f172a;border:1px solid #334155;'
            f'border-radius:4px;padding:1px 6px;font-size:11px;color:#e2e8f0;">'
            f'{score:.2f}</span>',
            unsafe_allow_html=True,
        )

        # Cover
        _render_cover(cover)

        # Video player
        if video:
            if st.session_state.get(play_key):
                st.video(video)
                if st.button("✕ Close", key=f"{play_key}_close",
                             use_container_width=True):
                    st.session_state[play_key] = False
                    st.rerun()
            else:
                if st.button("▶ Play", key=f"{play_key}_open",
                             use_container_width=True):
                    st.session_state[play_key] = True
                    st.rerun()

        # Title
        st.markdown(
            f'<p style="font-weight:600;font-size:13px;line-height:1.35;'
            f'display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;'
            f'overflow:hidden;margin:4px 0 2px;">{title}</p>',
            unsafe_allow_html=True,
        )

        # Stats
        c1, c2 = st.columns(2)
        c1.caption(f"❤️ {_fmt(likes)}")
        c2.caption(f"👁 {_fmt(views)}")

        # Reason
        st.markdown(
            f'<div style="font-size:11px;color:#a78bfa;margin:2px 0 4px;">'
            f'✦ {" · ".join(reason_tags)}</div>',
            unsafe_allow_html=True,
        )

        # Actions: More / Less / Hide
        a1, a2, a3 = st.columns(3)
        with a1:
            if st.button("👍", key=f"{key_prefix}_more_{item_id}",
                         use_container_width=True, help="More like this"):
                st.toast("Noted: more like this!", icon="👍")
        with a2:
            if st.button("👎", key=f"{key_prefix}_less_{item_id}",
                         use_container_width=True, help="Less like this"):
                st.toast("Noted: less like this!", icon="👎")
        with a3:
            if st.button("✕", key=hide_key,
                         use_container_width=True, help="Hide"):
                st.session_state[hide_key] = True
                st.rerun()
