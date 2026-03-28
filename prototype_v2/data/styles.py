"""
Design System — MUFG Brand v2
==============================
Professional banking UI with sidebar navigation.
"""
import streamlit as st
import base64, pathlib
from data.state_manager import STATUS_COLORS, STATUS_ICONS, dashboard_kpis, get_pending_approvals, get_queue_stats

# ──────────────────────────────────────────────────────────────────────────────
# COLOUR PALETTE
# ──────────────────────────────────────────────────────────────────────────────

P = {
    "primary":    "#D50032",
    "primary_dk": "#A80028",
    "primary_lt": "#FF3D5A",
    "accent":     "#1A1A2E",
    "grey_900":   "#212121",
    "grey_700":   "#616161",
    "grey_400":   "#BDBDBD",
    "grey_100":   "#F5F5F5",
    "white":      "#FFFFFF",
    "bg":         "#F7F8FA",
    "card":       "#FFFFFF",
    "border":     "#E8E8EC",
    "success":    "#2E7D32",
    "success_lt": "#E8F5E9",
    "warning":    "#E65100",
    "warning_lt": "#FFF3E0",
    "danger":     "#C62828",
    "danger_lt":  "#FFEBEE",
    "info":       "#1565C0",
    "info_lt":    "#E3F2FD",
    "purple":     "#6A1B9A",
    "purple_lt":  "#F3E5F5",
}

# ──────────────────────────────────────────────────────────────────────────────
# LOGO
# ──────────────────────────────────────────────────────────────────────────────

def _logo_b64() -> str:
    path = pathlib.Path(__file__).resolve().parent.parent / "assets" / "logo.svg"
    if path.exists():
        return base64.b64encode(path.read_bytes()).decode()
    return ""

# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ──────────────────────────────────────────────────────────────────────────────

def inject_css():
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* ── Base ───────────────────────────────────── */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    }}
    [data-testid="stAppViewContainer"] {{ background: {P["bg"]} !important; }}
    [data-testid="stMainBlockContainer"] {{ padding-top: 1.5rem; padding-bottom: 3rem; }}
    [data-testid="stVerticalBlock"] {{ gap: 0.75rem; }}

    /* ── Sidebar styling ────────────────────────── */
    [data-testid="stSidebar"] {{
        background: {P["accent"]} !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] > div:first-child {{
        background: {P["accent"]} !important;
        padding-top: 0 !important;
    }}
    [data-testid="stSidebar"] * {{ color: #E8E8EC !important; }}
    [data-testid="stSidebar"] hr {{ border-color: rgba(255,255,255,0.12) !important; }}
    [data-testid="stSidebarUserContent"] {{ padding: 0.5rem 0.75rem !important; }}

    /* sidebar radio (navigation) */
    [data-testid="stSidebar"] .stRadio label {{
        color: #C8CAD0 !important;
        font-size: 0.85rem !important;
        padding: 0.2rem 0 !important;
    }}
    [data-testid="stSidebar"] .stRadio [data-baseweb="radio"] {{
        display: none !important;
    }}

    /* ── Cards ──────────────────────────────────── */
    .mcard {{
        background: {P["white"]};
        border: 1px solid {P["border"]};
        border-radius: 10px;
        padding: 1.2rem 1.4rem;
        margin-bottom: 0.75rem;
        box-shadow: 0 1px 4px rgba(0,0,0,.04);
        transition: box-shadow .15s;
    }}
    .mcard:hover {{ box-shadow: 0 3px 12px rgba(0,0,0,.08); }}

    .mcard-accent {{
        border-left: 4px solid {P["primary"]};
    }}

    /* ── KPI metric cards ───────────────────────── */
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(155px, 1fr));
        gap: 14px;
        margin-bottom: 1.25rem;
    }}
    .kpi-card {{
        background: {P["white"]};
        border: 1px solid {P["border"]};
        border-radius: 10px;
        padding: 1rem 1.1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,.04);
    }}
    .kpi-card .kpi-label {{
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: .06em;
        color: {P["grey_700"]};
        margin-bottom: 0.35rem;
    }}
    .kpi-card .kpi-value {{
        font-size: 1.8rem;
        font-weight: 700;
        color: {P["accent"]};
        line-height: 1;
    }}
    .kpi-card .kpi-sub {{
        font-size: 0.72rem;
        color: {P["grey_700"]};
        margin-top: 0.25rem;
    }}
    .kpi-card.kpi-primary {{ border-top: 3px solid {P["primary"]}; }}
    .kpi-card.kpi-warning {{ border-top: 3px solid {P["warning"]}; }}
    .kpi-card.kpi-success {{ border-top: 3px solid {P["success"]}; }}
    .kpi-card.kpi-info    {{ border-top: 3px solid {P["info"]}; }}
    .kpi-card.kpi-purple  {{ border-top: 3px solid {P["purple"]}; }}

    /* ── Status badges ──────────────────────────── */
    .status-badge {{
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: .04em;
        text-transform: uppercase;
    }}

    /* ── Adjustment cards ───────────────────────── */
    .adj-card {{
        background: {P["white"]};
        border: 1px solid {P["border"]};
        border-radius: 10px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.6rem;
        box-shadow: 0 1px 3px rgba(0,0,0,.03);
    }}
    .adj-card-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }}
    .adj-id {{
        font-size: 0.8rem;
        font-weight: 700;
        color: {P["grey_700"]};
        letter-spacing: .04em;
    }}
    .adj-title {{
        font-size: 0.95rem;
        font-weight: 600;
        color: {P["accent"]};
    }}
    .adj-meta {{
        font-size: 0.78rem;
        color: {P["grey_700"]};
    }}
    .adj-filters {{
        display: flex;
        flex-wrap: wrap;
        gap: 5px;
        margin-top: 0.5rem;
    }}
    .filter-chip {{
        background: {P["grey_100"]};
        border-radius: 4px;
        padding: 2px 8px;
        font-size: 0.71rem;
        color: {P["grey_700"]};
        font-weight: 500;
    }}

    /* ── Overlap warning ────────────────────────── */
    .overlap-box {{
        background: {P["warning_lt"]};
        border: 1px solid #FFCC80;
        border-left: 4px solid {P["warning"]};
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin: 0.75rem 0;
    }}
    .overlap-box h4 {{
        color: {P["warning"]};
        margin: 0 0 0.5rem 0;
        font-size: 0.9rem;
    }}

    /* ── AI co-pilot panel ──────────────────────── */
    .ai-panel {{
        background: linear-gradient(135deg, {P["accent"]} 0%, #2D2D5E 100%);
        border-radius: 12px;
        padding: 1.2rem;
        color: #E8E8F8 !important;
        height: 100%;
        min-height: 300px;
    }}
    .ai-panel * {{ color: #E8E8F8 !important; }}
    .ai-header {{
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 0.75rem;
        padding-bottom: 0.6rem;
        border-bottom: 1px solid rgba(255,255,255,0.15);
    }}
    .ai-header span {{
        font-weight: 700;
        font-size: 0.85rem;
        letter-spacing: .05em;
        text-transform: uppercase;
    }}
    .ai-body {{
        font-size: 0.85rem;
        line-height: 1.6;
        opacity: 0.92;
    }}

    /* ── Timeline ───────────────────────────────── */
    .timeline {{ position: relative; padding-left: 24px; }}
    .timeline::before {{
        content: "";
        position: absolute;
        left: 8px; top: 0; bottom: 0;
        width: 2px;
        background: {P["border"]};
    }}
    .tl-item {{
        position: relative;
        margin-bottom: 1rem;
        padding: 0.6rem 0.8rem;
        background: {P["white"]};
        border: 1px solid {P["border"]};
        border-radius: 8px;
    }}
    .tl-item::before {{
        content: "";
        position: absolute;
        left: -20px; top: 14px;
        width: 10px; height: 10px;
        border-radius: 50%;
        background: {P["primary"]};
        border: 2px solid {P["white"]};
        box-shadow: 0 0 0 2px {P["primary"]};
    }}
    .tl-status {{
        font-size: 0.78rem;
        font-weight: 700;
        text-transform: uppercase;
    }}
    .tl-meta {{
        font-size: 0.73rem;
        color: {P["grey_700"]};
        margin-top: 2px;
    }}
    .tl-comment {{
        font-size: 0.8rem;
        color: {P["grey_900"]};
        margin-top: 4px;
        font-style: italic;
    }}

    /* ── Queue items ────────────────────────────── */
    .queue-item {{
        background: {P["white"]};
        border: 1px solid {P["border"]};
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
    }}
    .queue-item.running {{
        border-left: 3px solid {P["info"]};
        animation: pulse-border 2s infinite;
    }}
    .queue-item.pending {{
        border-left: 3px solid {P["warning"]};
    }}
    .queue-item.completed {{
        border-left: 3px solid {P["success"]};
        opacity: 0.8;
    }}
    .queue-item.failed {{
        border-left: 3px solid {P["danger"]};
    }}
    @keyframes pulse-border {{
        0%, 100% {{ border-left-color: {P["info"]}; }}
        50%       {{ border-left-color: #64B5F6; }}
    }}

    /* ── Pipeline diagram ───────────────────────── */
    .pipeline {{
        display: flex;
        align-items: center;
        gap: 0;
        overflow-x: auto;
        padding: 1rem;
        background: {P["grey_100"]};
        border-radius: 10px;
        margin: 1rem 0;
    }}
    .pipe-node {{
        background: {P["white"]};
        border: 2px solid {P["border"]};
        border-radius: 8px;
        padding: 0.6rem 0.9rem;
        text-align: center;
        min-width: 100px;
        flex-shrink: 0;
    }}
    .pipe-node.active {{
        border-color: {P["info"]};
        background: {P["info_lt"]};
    }}
    .pipe-node.done {{
        border-color: {P["success"]};
        background: {P["success_lt"]};
    }}
    .pipe-node .pn-icon {{ font-size: 1.3rem; }}
    .pipe-node .pn-label {{ font-size: 0.7rem; font-weight: 600; margin-top: 3px; color: {P["grey_700"]}; }}
    .pipe-arrow {{
        color: {P["grey_400"]};
        font-size: 1.2rem;
        padding: 0 4px;
        flex-shrink: 0;
    }}

    /* ── Scope selector cards ───────────────────── */
    .scope-grid {{
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 12px;
        margin-bottom: 1rem;
    }}
    .scope-card {{
        background: {P["white"]};
        border: 2px solid {P["border"]};
        border-radius: 12px;
        padding: 1.2rem;
        cursor: pointer;
        text-align: center;
        transition: all .15s;
    }}
    .scope-card:hover {{
        border-color: {P["primary"]};
        box-shadow: 0 4px 12px rgba(213,0,50,.12);
    }}
    .scope-card.selected {{
        border-color: {P["primary"]};
        background: #FFF0F3;
    }}
    .scope-card .sc-icon {{ font-size: 1.8rem; }}
    .scope-card .sc-title {{ font-weight: 700; font-size: 0.95rem; margin: 0.4rem 0 0.2rem; }}
    .scope-card .sc-sub {{ font-size: 0.73rem; color: {P["grey_700"]}; }}

    /* ── Step indicator ─────────────────────────── */
    .step-bar {{
        display: flex;
        align-items: center;
        gap: 0;
        margin-bottom: 1.5rem;
    }}
    .step {{
        display: flex;
        align-items: center;
        gap: 6px;
        flex: 1;
    }}
    .step-dot {{
        width: 26px; height: 26px;
        border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        font-size: 0.75rem;
        font-weight: 700;
        flex-shrink: 0;
    }}
    .step-dot.done    {{ background: {P["success"]}; color: white; }}
    .step-dot.active  {{ background: {P["primary"]}; color: white; }}
    .step-dot.pending {{ background: {P["grey_100"]}; color: {P["grey_700"]}; border: 2px solid {P["grey_400"]}; }}
    .step-label {{
        font-size: 0.78rem;
        font-weight: 600;
        white-space: nowrap;
    }}
    .step-label.done    {{ color: {P["success"]}; }}
    .step-label.active  {{ color: {P["primary"]}; }}
    .step-label.pending {{ color: {P["grey_700"]}; }}
    .step-line {{
        flex: 1;
        height: 2px;
        background: {P["border"]};
        margin: 0 6px;
    }}
    .step-line.done {{ background: {P["success"]}; }}

    /* ── Section headers ────────────────────────── */
    .section-title {{
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .08em;
        color: {P["grey_700"]};
        margin: 1rem 0 0.6rem 0;
        display: flex;
        align-items: center;
        gap: 6px;
    }}
    .section-title::after {{
        content: "";
        flex: 1;
        height: 1px;
        background: {P["border"]};
    }}

    /* ── Misc ───────────────────────────────────── */
    .tag {{
        display: inline-block;
        background: {P["info_lt"]};
        color: {P["info"]};
        border-radius: 4px;
        padding: 1px 7px;
        font-size: 0.7rem;
        font-weight: 600;
    }}
    .tag.recurring {{
        background: {P["purple_lt"]};
        color: {P["purple"]};
    }}
    .impact-up   {{ color: {P["success"]}; font-weight: 600; }}
    .impact-down {{ color: {P["danger"]};  font-weight: 600; }}
    .impact-zero {{ color: {P["grey_700"]}; }}
    div[data-testid="stExpander"] {{
        border: 1px solid {P["border"]};
        border-radius: 8px;
        overflow: hidden;
    }}
    .stButton>button {{
        border-radius: 7px !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }}
    [data-testid="stDataFrame"] {{ border-radius: 8px; overflow: hidden; }}
    </style>
    """, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ──────────────────────────────────────────────────────────────────────────────

def render_sidebar():
    from data.state_manager import current_user, USERS, set_current_user, can_admin
    from data.mock_data import ROLE_LABELS

    kpis = dashboard_kpis()
    q    = get_queue_stats()

    with st.sidebar:
        # ── Logo ──────────────────────────────────────────
        logo = _logo_b64()
        if logo:
            st.markdown(
                f'<div style="padding:1.2rem 0.5rem 0.8rem;border-bottom:1px solid rgba(255,255,255,0.1)">'
                f'<img src="data:image/svg+xml;base64,{logo}" height="30"/></div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="padding:1.1rem 0.5rem;border-bottom:1px solid rgba(255,255,255,0.1);'
                'font-size:1.1rem;font-weight:800;color:white;letter-spacing:.04em">MUFG</div>',
                unsafe_allow_html=True)

        # ── User selector ─────────────────────────────────
        st.markdown('<div style="padding:0.8rem 0 0.4rem;font-size:0.68rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.08em;color:rgba(255,255,255,0.45)">'
                    'Logged in as</div>', unsafe_allow_html=True)
        user_names = [u["name"] for u in USERS]
        user_ids   = [u["id"]   for u in USERS]
        cur = current_user()
        idx = user_ids.index(cur["id"]) if cur["id"] in user_ids else 0
        sel = st.selectbox("", user_names, index=idx, label_visibility="collapsed",
                           key="sidebar_user_sel")
        new_uid = user_ids[user_names.index(sel)]
        if new_uid != cur["id"]:
            set_current_user(new_uid)
            st.rerun()

        role_lbl, role_color = ROLE_LABELS.get(cur["role"], ("Operator", "#78909C"))
        st.markdown(
            f'<div style="margin-top:2px;">'
            f'<span style="background:{role_color};color:white;padding:2px 8px;'
            f'border-radius:4px;font-size:0.68rem;font-weight:700">{role_lbl}</span></div>',
            unsafe_allow_html=True)

        st.markdown("---")

        # ── Navigation ────────────────────────────────────
        st.markdown('<div style="font-size:0.68rem;font-weight:700;text-transform:uppercase;'
                    'letter-spacing:.08em;color:rgba(255,255,255,0.45);margin-bottom:0.5rem">'
                    'Navigation</div>', unsafe_allow_html=True)

        def nav_item(icon, label, badge=0, badge_color=P["warning"]):
            badge_html = ""
            if badge > 0:
                badge_html = (f'<span style="background:{badge_color};color:white;'
                              f'border-radius:10px;padding:0px 6px;font-size:0.65rem;'
                              f'font-weight:700;margin-left:auto">{badge}</span>')
            return st.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;'
                f'padding:0.45rem 0.3rem;border-radius:6px;cursor:pointer;'
                f'color:rgba(255,255,255,0.85);font-size:0.85rem">'
                f'{icon} {label}{badge_html}</div>',
                unsafe_allow_html=True)

        pages = [
            ("🏠", "Command Center",     "app",                   0,                        P["primary"]),
            ("✏️", "New Adjustment",     "pages/1_New_Adjustment","",                       ""),
            ("📋", "My Work",            "pages/2_My_Work",        kpis["my_pending"],       P["warning"]),
            ("✅", "Approval Queue",     "pages/3_Approval_Queue", kpis["pending_approval"], P["info"]),
            ("⏳", "Processing Queue",   "pages/4_Processing_Queue",q["pending"]+q["running"],P["success"]),
            ("🤖", "AI Insights",        "pages/5_AI_Insights",   0,                        ""),
            ("⚙️", "Admin",              "pages/6_Admin",         0,                        ""),
        ]
        for icon, label, page, badge, bc in pages:
            badge_html = (f'<span style="background:{bc};color:white;border-radius:10px;'
                          f'padding:0px 6px;font-size:0.65rem;font-weight:700;margin-left:auto">'
                          f'{badge}</span>') if badge else ""
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;padding:0.45rem 0.4rem;'
                f'border-radius:6px;color:rgba(255,255,255,0.82);font-size:0.84rem;'
                f'font-weight:500">{icon}&nbsp;{label}{badge_html}</div>',
                unsafe_allow_html=True)

        st.markdown("---")

        # ── Quick stats ───────────────────────────────────
        st.markdown('<div style="font-size:0.68rem;font-weight:700;text-transform:uppercase;'
                    'letter-spacing:.08em;color:rgba(255,255,255,0.45);margin-bottom:0.5rem">'
                    'Today\'s Snapshot</div>', unsafe_allow_html=True)

        def mini_stat(label, value, color="#E8E8F4"):
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f'padding:0.25rem 0;font-size:0.78rem">'
                f'<span style="color:rgba(255,255,255,0.6)">{label}</span>'
                f'<span style="color:{color};font-weight:700">{value}</span></div>',
                unsafe_allow_html=True)

        mini_stat("Total adjustments", kpis["total"])
        mini_stat("Pending approval",  kpis["pending_approval"], "#FFB74D")
        mini_stat("Applied",           kpis["applied"],          "#81C784")
        mini_stat("Queue active",      q["pending"] + q["running"], "#64B5F6")

# ──────────────────────────────────────────────────────────────────────────────
# REUSABLE COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#9E9E9E")
    icon  = STATUS_ICONS.get(status, "•")
    bg    = color + "22"
    return (f'<span class="status-badge" style="background:{bg};color:{color}">'
            f'{icon} {status.replace("_", " ")}</span>')


def kpi_card(label: str, value, sub: str = "", variant: str = "primary") -> str:
    return (f'<div class="kpi-card kpi-{variant}">'
            f'<div class="kpi-label">{label}</div>'
            f'<div class="kpi-value">{value}</div>'
            f'<div class="kpi-sub">{sub}</div>'
            f'</div>')


def section_title(text: str, icon: str = ""):
    prefix = f"{icon} " if icon else ""
    st.markdown(f'<div class="section-title">{prefix}{text}</div>', unsafe_allow_html=True)


def fmt_number(v, fmt="currency") -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    if fmt == "currency":
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:+,.2f}M"
        elif abs(v) >= 1_000:
            return f"${v/1_000:+,.1f}K"
        return f"${v:+,.0f}"
    elif fmt == "number":
        if abs(v) >= 1_000_000:
            return f"{v/1_000_000:+,.2f}M"
        return f"{v:+,.2f}"
    return str(v)


def render_step_bar(current_step: int, steps: list[str]):
    dots, lines = [], []
    for i, label in enumerate(steps, 1):
        state = "done" if i < current_step else ("active" if i == current_step else "pending")
        icon  = "✓" if state == "done" else str(i)
        dots.append(
            f'<div class="step">'
            f'<div class="step-dot {state}">{icon}</div>'
            f'<span class="step-label {state}">{label}</span>'
            f'</div>')
        if i < len(steps):
            line_state = "done" if i < current_step else ""
            dots.append(f'<div class="step-line {line_state}"></div>')
    st.markdown(f'<div class="step-bar">{"".join(dots)}</div>', unsafe_allow_html=True)


def render_filter_chips(filter_criteria: dict, scope_key: str):
    from data.mock_data import SCOPES
    scope = SCOPES.get(scope_key, {})
    dim_map = {d["column"]: d["label"] for d in scope.get("dimensions", [])}
    chips = []
    for col, vals in filter_criteria.items():
        if vals:
            label = dim_map.get(col, col)
            chips.append(f'<span class="filter-chip">{label}: {", ".join(str(v) for v in vals)}</span>')
    if chips:
        st.markdown(f'<div class="adj-filters">{"".join(chips)}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="font-size:0.78rem;color:#9E9E9E">All records (no filters)</span>',
                    unsafe_allow_html=True)


def render_status_timeline(history: list[dict]):
    if not history:
        st.info("No status history available.")
        return
    st.markdown('<div class="timeline">', unsafe_allow_html=True)
    for h in sorted(history, key=lambda x: x["changed_at"]):
        status = h["new_status"]
        color  = STATUS_COLORS.get(status, "#9E9E9E")
        icon   = STATUS_ICONS.get(status, "•")
        ts     = h["changed_at"].strftime("%d %b %Y %H:%M") if hasattr(h["changed_at"], "strftime") else str(h["changed_at"])
        st.markdown(
            f'<div class="tl-item">'
            f'<div class="tl-status" style="color:{color}">{icon} {status.replace("_", " ")}</div>'
            f'<div class="tl-meta">by {h["changed_by"]} · {ts}</div>'
            + (f'<div class="tl-comment">"{h["comment"]}"</div>' if h.get("comment") else "")
            + '</div>',
            unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def render_ai_panel(content: str, title: str = "AI Co-pilot"):
    st.markdown(
        f'<div class="ai-panel">'
        f'<div class="ai-header">🤖 <span>{title}</span></div>'
        f'<div class="ai-body">\n\n{content}\n\n</div>'
        f'</div>',
        unsafe_allow_html=True)


def render_overlap_warning(overlaps: list[dict]):
    if not overlaps:
        return
    n = len(overlaps)
    lines = []
    for o in overlaps:
        a = o["adj"]
        lines.append(
            f'<div style="margin:6px 0;padding:6px 8px;background:rgba(255,255,255,0.7);'
            f'border-radius:6px;font-size:0.82rem">'
            f'<strong>ADJ #{a["adj_id"]}</strong> &nbsp;'
            f'<span style="color:#E65100;font-weight:600">{a["adj_type"]}</span> · '
            f'{a["adj_status"].replace("_", " ")} · by {a["created_by"]}<br/>'
            f'<span style="color:#757575">{o["rows_in_common"]} records in common on this COB</span>'
            f'</div>'
        )
    st.markdown(
        f'<div class="overlap-box">'
        f'<h4>⚠️ {n} Overlapping Adjustment{"s" if n > 1 else ""} Found</h4>'
        f'{"".join(lines)}'
        f'<div style="margin-top:8px;font-size:0.8rem;color:#E65100">'
        f'Proceeding will create overlapping adjustments. '
        f'Ensure this is intentional or coordinate with the other adjustment\'s owner.</div>'
        f'</div>',
        unsafe_allow_html=True)


def render_pipeline_diagram(current_stage: int = 5):
    """Render Snowflake pipeline visualization (0=idle, 1-5 = stages)."""
    stages = [
        ("💾", "ADJ Header\nInsert"),
        ("🌊", "Stream\nCapture"),
        ("⚙️",  "Task\nWakes Up"),
        ("🔄", "SP\nExecutes"),
        ("📊", "Dynamic Table\nRefresh"),
    ]
    nodes = []
    for i, (icon, label) in enumerate(stages, 1):
        state_class = "done" if i < current_stage else ("active" if i == current_stage else "")
        nodes.append(f'<div class="pipe-node {state_class}"><div class="pn-icon">{icon}</div>'
                     f'<div class="pn-label">{label}</div></div>')
        if i < len(stages):
            nodes.append('<div class="pipe-arrow">→</div>')
    st.markdown(f'<div class="pipeline">{"".join(nodes)}</div>', unsafe_allow_html=True)
