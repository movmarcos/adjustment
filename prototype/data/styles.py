"""
UI Theme & Reusable Components
===============================
CSS injections + helper functions for a polished, professional look.
"""
import streamlit as st
from data.state_manager import STATUS_COLORS

# ─────────────────────────────────────────────────────────────────────
# COLOUR PALETTE
# ─────────────────────────────────────────────────────────────────────
PALETTE = {
    "primary":    "#0D47A1",
    "primary_lt": "#1565C0",
    "accent":     "#00BFA5",
    "bg":         "#F5F7FA",
    "card":       "#FFFFFF",
    "border":     "#E0E4EA",
    "text":       "#263238",
    "text_sec":   "#607D8B",
    "success":    "#66BB6A",
    "warning":    "#FFA726",
    "danger":     "#EF5350",
    "info":       "#29B6F6",
}


# ─────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ─────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    /* ── Base ───────────────────────────────────────── */
    [data-testid="stAppViewContainer"] { background: #F5F7FA; }
    section[data-testid="stSidebar"] { background: #0D47A1 !important; }
    section[data-testid="stSidebar"] * { color: #FFFFFF !important; }
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stRadio label { color: rgba(255,255,255,.85) !important; }

    /* ── Cards ──────────────────────────────────────── */
    .card {
        background: #fff; border-radius: 12px;
        border: 1px solid #E0E4EA; padding: 20px 24px;
        margin-bottom: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,.06);
        transition: box-shadow .15s;
    }
    .card:hover { box-shadow: 0 4px 12px rgba(0,0,0,.1); }

    /* ── Metric cards ─────────────────────────────── */
    .metric-card { text-align: center; padding: 18px 12px; }
    .metric-card .value {
        font-size: 2rem; font-weight: 700;
        color: #0D47A1; line-height: 1.1;
    }
    .metric-card .label {
        font-size: .8rem; text-transform: uppercase;
        letter-spacing: .5px; color: #607D8B; margin-top: 6px;
    }
    .metric-card .delta {
        font-size: .75rem; margin-top: 4px;
    }
    .delta-pos { color: #66BB6A; }
    .delta-neg { color: #EF5350; }

    /* ── Badges ─────────────────────────────────────── */
    .badge {
        display: inline-block; padding: 3px 10px;
        border-radius: 20px; font-size: .72rem;
        font-weight: 600; letter-spacing: .3px;
        text-transform: uppercase;
    }

    /* ── Section headers ──────────────────────────── */
    .section-header {
        font-size: 1.1rem; font-weight: 600;
        color: #263238; margin: 24px 0 12px 0;
        padding-bottom: 8px; border-bottom: 2px solid #0D47A1;
        display: inline-block;
    }

    /* ── Scope pill bar ──────────────────────────── */
    .scope-bar { display: flex; gap: 8px; margin-bottom: 16px; }
    .scope-pill {
        padding: 8px 18px; border-radius: 24px;
        font-size: .85rem; font-weight: 600;
        cursor: pointer; border: 2px solid #E0E4EA;
        background: #fff; color: #263238;
        transition: all .15s;
    }
    .scope-pill.active {
        background: #0D47A1; color: #fff;
        border-color: #0D47A1;
    }

    /* ── Timeline ────────────────────────────────── */
    .tl-item {
        padding: 10px 0 10px 28px;
        border-left: 2px solid #E0E4EA;
        position: relative;
    }
    .tl-item::before {
        content: ''; position: absolute; left: -6px; top: 14px;
        width: 10px; height: 10px; border-radius: 50%;
        background: #0D47A1;
    }
    .tl-item .tl-time { font-size: .72rem; color: #607D8B; }
    .tl-item .tl-text { font-size: .85rem; color: #263238; }

    /* ── Table tweaks ───────────────────────────── */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* ── Hide default hamburger & footer ─────── */
    #MainMenu, footer { display: none; }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# COMPONENT HELPERS
# ─────────────────────────────────────────────────────────────────────

def metric_card(label: str, value, delta: str | None = None, prefix: str = ""):
    """Render a styled metric card."""
    delta_html = ""
    if delta:
        cls = "delta-pos" if not delta.startswith("-") else "delta-neg"
        delta_html = f'<div class="delta {cls}">{delta}</div>'
    return f"""
    <div class="card metric-card">
        <div class="value">{prefix}{value}</div>
        <div class="label">{label}</div>
        {delta_html}
    </div>
    """


def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#78909C")
    return f'<span class="badge" style="background:{color}22;color:{color};border:1px solid {color}44">{status.replace("_"," ")}</span>'


def section_header(text: str):
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


def scope_selector_sidebar():
    """Render scope selector in sidebar and return current scope id."""
    from data.mock_data import SCOPES, get_scope_choices
    choices = get_scope_choices()
    labels = [f"{c['icon']}  {c['name']}" for c in choices]
    ids = [c["id"] for c in choices]
    current = st.session_state.get("current_scope", "pnl")
    idx = ids.index(current) if current in ids else 0

    with st.sidebar:
        st.markdown("### 🎯 Scope")
        selected_label = st.radio(
            "Select fact table",
            labels,
            index=idx,
            label_visibility="collapsed",
        )
        sel_idx = labels.index(selected_label)
        st.session_state["current_scope"] = ids[sel_idx]

        cfg = SCOPES[ids[sel_idx]]
        st.caption(cfg["description"])
        st.markdown("---")

        st.markdown("### 👤 User")
        user = st.selectbox(
            "Switch user",
            ["Sarah", "James", "Manager", "Admin"],
            index=["Sarah", "James", "Manager", "Admin"].index(
                st.session_state.get("current_user", "Sarah")
            ),
            label_visibility="collapsed",
        )
        st.session_state["current_user"] = user
    return ids[sel_idx]


def format_number(val, prefix="$", compact=True) -> str:
    """Format a number for display."""
    if pd.isna(val):
        return "—"
    try:
        v = float(val)
    except (ValueError, TypeError):
        return str(val)
    if compact and abs(v) >= 1_000_000:
        return f"{prefix}{v/1_000_000:,.1f}M"
    elif compact and abs(v) >= 1_000:
        return f"{prefix}{v/1_000:,.1f}K"
    return f"{prefix}{v:,.0f}"


import pandas as pd
