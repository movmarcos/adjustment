"""
UI Theme & Reusable Components
===============================
MUFG-branded theme with top navigation bar.
"""
import streamlit as st
import base64, os, pathlib
from _OLD.prototype.data.state_manager import STATUS_COLORS
import pandas as pd

# ─────────────────────────────────────────────────────────────────────
# COLOUR PALETTE  (MUFG brand: crimson red + charcoal gray)
# ─────────────────────────────────────────────────────────────────────
PALETTE = {
    "primary":    "#D50032",   # MUFG red
    "primary_dk": "#A80028",
    "primary_lt": "#E8335A",
    "accent":     "#58595B",   # MUFG charcoal
    "accent_lt":  "#747678",
    "bg":         "#F7F7F8",
    "card":       "#FFFFFF",
    "border":     "#E5E5E7",
    "text":       "#2D2D2D",
    "text_sec":   "#6B6B6B",
    "success":    "#00875A",
    "warning":    "#E68A00",
    "danger":     "#D50032",
    "info":       "#0065BD",
}

# ─────────────────────────────────────────────────────────────────────
# LOGO HELPER
# ─────────────────────────────────────────────────────────────────────
_LOGO_B64 = None

def _get_logo_b64() -> str:
    global _LOGO_B64
    if _LOGO_B64 is None:
        logo_path = pathlib.Path(__file__).resolve().parent.parent / "assets" / "logo.svg"
        if logo_path.exists():
            _LOGO_B64 = base64.b64encode(logo_path.read_bytes()).decode()
        else:
            _LOGO_B64 = ""
    return _LOGO_B64


# ─────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ─────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    /* ── Import Inter font ─────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* ── Base ───────────────────────────────────────── */
    html, body, [class*="css"] { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important; }
    [data-testid="stAppViewContainer"] { background: #F7F7F8; }

    /* ── Hide sidebar completely ─────────────────── */
    section[data-testid="stSidebar"] { display: none !important; }
    button[data-testid="stSidebarCollapsedControl"],
    [data-testid="collapsedControl"] { display: none !important; }

    /* ── Top navigation bar ─────────────────────── */
    .mufg-topbar {
        background: #FFFFFF;
        border-bottom: 1px solid #E5E5E7;
        padding: 0 24px;
        display: flex; align-items: center; gap: 0;
        height: 56px;
        position: sticky; top: 0; z-index: 999;
        margin: -1rem -1rem 20px -1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,.04);
    }
    .mufg-topbar .logo-area {
        display: flex; align-items: center; gap: 12px;
        padding-right: 28px; border-right: 1px solid #E5E5E7;
        height: 100%;
    }
    .mufg-topbar .logo-area img { height: 28px; }
    .mufg-topbar .logo-area .app-name {
        font-size: .72rem; color: #6B6B6B; font-weight: 500;
        text-transform: uppercase; letter-spacing: .8px;
    }
    .mufg-topbar nav { display: flex; align-items: center; gap: 0; height: 100%; padding-left: 4px; }
    .mufg-topbar nav a {
        text-decoration: none; color: #58595B;
        font-size: .85rem; font-weight: 500;
        padding: 0 18px; height: 100%;
        display: flex; align-items: center;
        border-bottom: 3px solid transparent;
        transition: all .15s;
    }
    .mufg-topbar nav a:hover { color: #D50032; background: #FFF5F7; }
    .mufg-topbar nav a.active {
        color: #D50032; border-bottom-color: #D50032; font-weight: 600;
    }
    .mufg-topbar .right-area {
        margin-left: auto; display: flex; align-items: center; gap: 14px;
        font-size: .82rem; color: #6B6B6B;
    }
    .mufg-topbar .scope-tag {
        background: #D500320F; color: #D50032; font-weight: 600;
        padding: 4px 12px; border-radius: 16px; font-size: .75rem;
    }
    .mufg-topbar .user-tag {
        background: #58595B12; color: #58595B; font-weight: 500;
        padding: 4px 12px; border-radius: 16px; font-size: .75rem;
    }

    /* ── Cards ──────────────────────────────────────── */
    .card {
        background: #fff; border-radius: 10px;
        border: 1px solid #E5E5E7; padding: 20px 24px;
        margin-bottom: 12px;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
        transition: box-shadow .15s;
    }
    .card:hover { box-shadow: 0 4px 14px rgba(0,0,0,.07); }

    /* ── Metric cards ─────────────────────────────── */
    .metric-card { text-align: center; padding: 18px 12px; }
    .metric-card .value {
        font-size: 1.9rem; font-weight: 700;
        color: #2D2D2D; line-height: 1.1;
    }
    .metric-card .label {
        font-size: .78rem; text-transform: uppercase;
        letter-spacing: .5px; color: #6B6B6B; margin-top: 6px;
        font-weight: 500;
    }
    .metric-card .delta { font-size: .75rem; margin-top: 4px; font-weight: 600; }
    .delta-pos { color: #00875A; }
    .delta-neg { color: #D50032; }

    /* ── Badges ─────────────────────────────────────── */
    .badge {
        display: inline-block; padding: 3px 10px;
        border-radius: 20px; font-size: .72rem;
        font-weight: 600; letter-spacing: .3px;
        text-transform: uppercase;
    }

    /* ── Section headers ──────────────────────────── */
    .section-header {
        font-size: 1.05rem; font-weight: 600;
        color: #2D2D2D; margin: 24px 0 12px 0;
        padding-bottom: 8px; border-bottom: 2px solid #D50032;
        display: inline-block;
    }

    /* ── Timeline ────────────────────────────────── */
    .tl-item {
        padding: 10px 0 10px 28px;
        border-left: 2px solid #E5E5E7;
        position: relative;
    }
    .tl-item::before {
        content: ''; position: absolute; left: -6px; top: 14px;
        width: 10px; height: 10px; border-radius: 50%;
        background: #D50032;
    }
    .tl-item .tl-time { font-size: .72rem; color: #6B6B6B; }
    .tl-item .tl-text { font-size: .85rem; color: #2D2D2D; }

    /* ── Table tweaks ───────────────────────────── */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Streamlit button override for MUFG red */
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="stBaseButton-primary"] {
        background-color: #D50032 !important;
        border-color: #D50032 !important;
    }
    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="stBaseButton-primary"]:hover {
        background-color: #A80028 !important;
        border-color: #A80028 !important;
    }

    /* ── Hide default hamburger & footer ─────── */
    #MainMenu, footer { display: none; }
    header[data-testid="stHeader"] { display: none !important; }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# TOP NAVIGATION BAR
# ─────────────────────────────────────────────────────────────────────
_NAV_ITEMS = [
    ("Home",             "🏠", "/"),
    ("Apply Adjustment", "📝", "/Apply_Adjustment"),
    ("Audit Trail",      "📋", "/Audit_Trail"),
    ("AI Assistant",     "🤖", "/AI_Assistant"),
    ("Dashboard",        "📊", "/Dashboard"),
]

def top_navbar(active_page: str = "Home"):
    """Render the MUFG top navigation bar with scope/user controls."""
    from _OLD.prototype.data.mock_data import SCOPES, get_scope_choices

    logo_b64 = _get_logo_b64()
    logo_html = f'<img src="data:image/svg+xml;base64,{logo_b64}" alt="MUFG"/>' if logo_b64 else '<span style="font-weight:700;color:#D50032;font-size:1.2rem">MUFG</span>'

    nav_links = ""
    for label, icon, href in _NAV_ITEMS:
        cls = "active" if label == active_page else ""
        nav_links += f'<a href="{href}" target="_self" class="{cls}">{icon}&nbsp;&nbsp;{label}</a>'

    scope_id = st.session_state.get("current_scope", "pnl")
    scope_cfg = SCOPES.get(scope_id, SCOPES["pnl"])
    user = st.session_state.get("current_user", "Sarah")

    st.markdown(f"""
    <div class="mufg-topbar">
        <div class="logo-area">
            {logo_html}
            <span class="app-name">Adjustment Engine</span>
        </div>
        <nav>{nav_links}</nav>
        <div class="right-area">
            <span class="scope-tag">{scope_cfg['icon']} {scope_cfg['name']}</span>
            <span class="user-tag">👤 {user}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def scope_and_user_controls():
    """Render scope selector + user switcher as inline controls below the navbar."""
    from _OLD.prototype.data.mock_data import SCOPES, get_scope_choices
    choices = get_scope_choices()
    labels = [f"{c['icon']}  {c['name']}" for c in choices]
    ids = [c["id"] for c in choices]
    current = st.session_state.get("current_scope", "pnl")
    idx = ids.index(current) if current in ids else 0

    c1, c2, c3 = st.columns([2, 2, 8])
    with c1:
        selected_label = st.selectbox(
            "Scope",
            labels,
            index=idx,
            label_visibility="collapsed",
            key="scope_control",
        )
        sel_idx = labels.index(selected_label)
        st.session_state["current_scope"] = ids[sel_idx]
    with c2:
        user = st.selectbox(
            "User",
            ["Sarah", "James", "Manager", "Admin"],
            index=["Sarah", "James", "Manager", "Admin"].index(
                st.session_state.get("current_user", "Sarah")
            ),
            label_visibility="collapsed",
            key="user_control",
        )
        st.session_state["current_user"] = user

    return ids[sel_idx]


# Keep backward compat — old pages calling scope_selector_sidebar
def scope_selector_sidebar():
    """DEPRECATED — redirects to scope_and_user_controls."""
    return scope_and_user_controls()


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
