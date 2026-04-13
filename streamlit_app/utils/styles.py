"""
Design System — MUFG Brand
============================
Professional banking UI. Colours, CSS, and reusable components.
Adapted from prototype_v2 design system for production use.
"""
import streamlit as st

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
# STATUS MAPPING
# ──────────────────────────────────────────────────────────────────────────────

STATUS_COLORS = {
    "Pending":              "#FB8C00",
    "Pending Approval":     "#1565C0",
    "Approved":             "#00897B",
    "Running":              "#1565C0",
    "Processed":            "#388E3C",
    "Failed":               "#D32F2F",
    "Rejected":             "#C62828",
    "Rejected - SignedOff": "#7B1FA2",
    "Deleted":              "#757575",
}

STATUS_ICONS = {
    "Pending":              "⏳",
    "Pending Approval":     "📝",
    "Approved":             "✅",
    "Running":              "⚡",
    "Processed":            "✔️",
    "Failed":               "❌",
    "Rejected":             "❌",
    "Rejected - SignedOff": "🔒",
    "Deleted":              "🗑️",
}

SCOPE_CONFIG = {
    "VaR":         {"icon": "📊", "color": "#D50032", "bg": "#FFF0F3", "label": "VaR"},
    "Stress":      {"icon": "⚡", "color": "#1565C0", "bg": "#E3F2FD", "label": "Stress"},
    "FRTB":        {"icon": "🏛️", "color": "#2E7D32", "bg": "#E8F5E9", "label": "FRTB"},
    "Sensitivity": {"icon": "🎯", "color": "#E65100", "bg": "#FFF3E0", "label": "Sensitivity"},
}

TYPE_CONFIG = {
    "Flatten": {"icon": "🔴", "desc": "Zero out matching positions", "formula": "new = original × 0"},
    "Scale":   {"icon": "📊", "desc": "Multiply by a scale factor", "formula": "new = original × sf"},
    "Roll":    {"icon": "🔄", "desc": "Copy from prior COB and scale", "formula": "new = prior × sf"},
}

# ── Adjustment Category — the first selection in the wizard ──────────────────

CATEGORY_CONFIG = {
    "Scaling Adjustment": {
        "icon": "⚖️", "color": "#D50032", "bg": "#FFF0F3",
        "desc": "Scale, flatten or roll fact table data (config-driven)",
    },
    "VaR Upload": {
        "icon": "📤", "color": "#6A1B9A", "bg": "#F3E5F5",
        "desc": "Upload a CSV with 21 VaR measure columns for direct insertion",
    },
    "Entity Roll": {
        "icon": "🔄", "color": "#E65100", "bg": "#FFF3E0",
        "desc": "Full entity roll — copies all data from a source COB (approval required)",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ──────────────────────────────────────────────────────────────────────────────

def inject_css():
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    }}
    [data-testid="stAppViewContainer"] {{ background: {P["bg"]} !important; }}
    [data-testid="stMainBlockContainer"] {{ padding-top: 1.5rem; padding-bottom: 3rem; }}
    [data-testid="stVerticalBlock"] {{ gap: 0.75rem; }}

    /* ── Sidebar — MUFG brand dark grey ────────────────────────────────── */
    [data-testid="stSidebar"] {{
        background: #3C3D3E !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] > div:first-child {{
        background: #3C3D3E !important;
        overflow: hidden !important;   /* prevent sidebar scroll */
        height: 100vh !important;
        display: flex !important;
        flex-direction: column !important;
    }}
    [data-testid="stSidebar"] * {{ color: #E8E8EC !important; }}
    [data-testid="stSidebar"] hr {{ border-color: rgba(255,255,255,0.12) !important; }}

    /* Logo area red top-stripe */
    [data-testid="stSidebar"] > div:first-child::before {{
        content: "";
        display: block;
        height: 3px;
        background: #D50032;
        flex-shrink: 0;
    }}

    /* Compact sidebar navigation — all pages visible with no scroll */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] {{
        padding-top: 0.2rem !important;
        flex-shrink: 0 !important;
        max-height: none !important;   /* override Streamlit's built-in scroll cap */
        overflow: visible !important;
        height: auto !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] ul {{
        padding-bottom: 0.2rem !important;
        max-height: none !important;
        overflow: visible !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] li {{
        margin: 0 !important;
        padding: 0 !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a {{
        padding: 0.3rem 0.9rem !important;
        font-size: 0.83rem !important;
        line-height: 1.25 !important;
        min-height: unset !important;
        border-radius: 0 !important;
        transition: background .12s !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {{
        background: rgba(213,0,50,.15) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-selected="true"] {{
        background: rgba(213,0,50,.22) !important;
        border-left: 3px solid #D50032 !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a span {{
        font-size: 0.83rem !important;
        white-space: nowrap !important;
    }}

    /* Flex chain: sidebar inner blocks must pass flex context down so that
       the spacer div (flex:1) can actually push the user footer to the bottom. */
    [data-testid="stSidebar"] > div:first-child > div,
    [data-testid="stSidebar"] > div:first-child > div > div,
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{
        display: flex !important;
        flex-direction: column !important;
        flex: 1 !important;
        min-height: 0 !important;
    }}
    /* stSidebarNav must not stretch — it's a fixed-size block */
    [data-testid="stSidebarNav"] {{
        flex: 0 0 auto !important;
    }}

    /* Sidebar user footer — pushed to bottom */
    .sidebar-user-footer {{
        border-top: 1px solid rgba(255,255,255,0.1);
        padding: 0.65rem 1rem;
        background: #343536;
        flex-shrink: 0 !important;
    }}

    /* Cards */
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

    /* KPI cards */
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
        font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: .06em; color: {P["grey_700"]}; margin-bottom: 0.35rem;
    }}
    .kpi-card .kpi-value {{ font-size: 1.8rem; font-weight: 700; color: {P["accent"]}; line-height: 1; }}
    .kpi-card .kpi-sub {{ font-size: 0.72rem; color: {P["grey_700"]}; margin-top: 0.25rem; }}
    .kpi-card.kpi-primary {{ border-top: 3px solid {P["primary"]}; }}
    .kpi-card.kpi-warning {{ border-top: 3px solid {P["warning"]}; }}
    .kpi-card.kpi-success {{ border-top: 3px solid {P["success"]}; }}
    .kpi-card.kpi-info    {{ border-top: 3px solid {P["info"]}; }}
    .kpi-card.kpi-purple  {{ border-top: 3px solid {P["purple"]}; }}
    .kpi-card.kpi-danger  {{ border-top: 3px solid {P["danger"]}; }}

    /* Status badges */
    .status-badge {{
        display: inline-block; padding: 2px 10px; border-radius: 20px;
        font-size: 0.72rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase;
    }}

    /* Adjustment cards */
    .adj-card {{
        background: {P["white"]}; border: 1px solid {P["border"]};
        border-radius: 10px; padding: 1rem 1.2rem; margin-bottom: 0.6rem;
        box-shadow: 0 1px 3px rgba(0,0,0,.03);
    }}
    .adj-card-header {{
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem;
    }}
    .adj-id {{ font-size: 0.8rem; font-weight: 700; color: {P["grey_700"]}; letter-spacing: .04em; }}
    .adj-title {{ font-size: 0.95rem; font-weight: 600; color: {P["accent"]}; }}
    .adj-meta {{ font-size: 0.78rem; color: {P["grey_700"]}; }}
    .adj-filters {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 0.5rem; }}
    .filter-chip {{
        background: {P["grey_100"]}; border-radius: 4px; padding: 2px 8px;
        font-size: 0.71rem; color: {P["grey_700"]}; font-weight: 500;
    }}

    /* Overlap warning */
    .overlap-box {{
        background: {P["warning_lt"]}; border: 1px solid #FFCC80;
        border-left: 4px solid {P["warning"]}; border-radius: 8px;
        padding: 1rem 1.2rem; margin: 0.75rem 0;
    }}
    .overlap-box h4 {{ color: {P["warning"]}; margin: 0 0 0.5rem 0; font-size: 0.9rem; }}

    /* Timeline */
    .timeline {{ position: relative; padding-left: 24px; }}
    .timeline::before {{
        content: ""; position: absolute; left: 8px; top: 0; bottom: 0;
        width: 2px; background: {P["border"]};
    }}
    .tl-item {{
        position: relative; margin-bottom: 1rem; padding: 0.6rem 0.8rem;
        background: {P["white"]}; border: 1px solid {P["border"]}; border-radius: 8px;
    }}
    .tl-item::before {{
        content: ""; position: absolute; left: -20px; top: 14px;
        width: 10px; height: 10px; border-radius: 50%;
        background: {P["grey_400"]}; border: 2px solid {P["white"]};
        box-shadow: 0 0 0 2px {P["border"]};
    }}
    .tl-status {{ font-size: 0.78rem; font-weight: 700; text-transform: uppercase; }}
    .tl-meta {{ font-size: 0.73rem; color: {P["grey_700"]}; margin-top: 2px; }}
    .tl-comment {{ font-size: 0.8rem; color: {P["grey_900"]}; margin-top: 4px; font-style: italic; }}

    /* Queue items */
    .queue-item {{
        background: {P["white"]}; border: 1px solid {P["border"]};
        border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 0.5rem;
    }}
    .queue-item.running {{ border-left: 3px solid {P["info"]}; }}
    .queue-item.pending {{ border-left: 3px solid {P["warning"]}; }}
    .queue-item.completed {{ border-left: 3px solid {P["success"]}; opacity: 0.85; }}
    .queue-item.failed {{ border-left: 3px solid {P["danger"]}; }}

    /* Pipeline diagram */
    .pipeline {{
        display: flex; align-items: center; gap: 0; overflow-x: auto;
        padding: 1rem; background: {P["grey_100"]}; border-radius: 10px; margin: 1rem 0;
    }}
    .pipe-node {{
        background: {P["white"]}; border: 2px solid {P["border"]};
        border-radius: 8px; padding: 0.6rem 0.9rem; text-align: center;
        min-width: 100px; flex-shrink: 0;
    }}
    .pipe-node.active {{ border-color: {P["info"]}; background: {P["info_lt"]}; }}
    .pipe-node.done {{ border-color: {P["success"]}; background: {P["success_lt"]}; }}
    .pipe-node .pn-icon {{ font-size: 1.3rem; }}
    .pipe-node .pn-label {{ font-size: 0.7rem; font-weight: 600; margin-top: 3px; color: {P["grey_700"]}; }}
    .pipe-arrow {{ color: {P["grey_400"]}; font-size: 1.2rem; padding: 0 4px; flex-shrink: 0; }}

    /* Step indicator */
    .step-bar {{ display: flex; align-items: center; gap: 0; margin-bottom: 1.5rem; }}
    .step {{ display: flex; align-items: center; gap: 6px; flex: 1; }}
    .step-dot {{
        width: 26px; height: 26px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        font-size: 0.75rem; font-weight: 700; flex-shrink: 0;
    }}
    .step-dot.done    {{ background: {P["success"]}; color: white; }}
    .step-dot.active  {{ background: {P["primary"]}; color: white; }}
    .step-dot.pending {{ background: {P["grey_100"]}; color: {P["grey_700"]}; border: 2px solid {P["grey_400"]}; }}
    .step-label {{ font-size: 0.78rem; font-weight: 600; white-space: nowrap; }}
    .step-label.done    {{ color: {P["success"]}; }}
    .step-label.active  {{ color: {P["primary"]}; }}
    .step-label.pending {{ color: {P["grey_700"]}; }}
    .step-line {{ flex: 1; height: 2px; background: {P["border"]}; margin: 0 6px; }}
    .step-line.done {{ background: {P["success"]}; }}

    /* Section headers */
    .section-title {{
        font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: .08em; color: {P["grey_700"]}; margin: 1rem 0 0.6rem 0;
        display: flex; align-items: center; gap: 6px;
    }}
    .section-title::after {{ content: ""; flex: 1; height: 1px; background: {P["border"]}; }}

    /* Tags */
    .tag {{
        display: inline-block; background: {P["info_lt"]}; color: {P["info"]};
        border-radius: 4px; padding: 1px 7px; font-size: 0.7rem; font-weight: 600;
    }}
    .tag.recurring {{ background: {P["purple_lt"]}; color: {P["purple"]}; }}

    /* Misc */
    .stButton>button {{ border-radius: 7px !important; font-weight: 600 !important; font-size: 0.85rem !important; }}
    [data-testid="stDataFrame"] {{ border-radius: 8px; overflow: hidden; }}
    div[data-testid="stExpander"] {{ border: 1px solid {P["border"]}; border-radius: 8px; overflow: hidden; }}
    </style>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# REUSABLE COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#9E9E9E")
    icon  = STATUS_ICONS.get(status, "•")
    bg    = color + "22"
    return (f'<span class="status-badge" style="background:{bg};color:{color}">'
            f'{icon} {status}</span>')


def kpi_card(label: str, value, sub: str = "", variant: str = "primary") -> str:
    return (f'<div class="kpi-card kpi-{variant}">'
            f'<div class="kpi-label">{label}</div>'
            f'<div class="kpi-value">{value}</div>'
            f'<div class="kpi-sub">{sub}</div>'
            f'</div>')


def section_title(text: str, icon: str = ""):
    prefix = f"{icon} " if icon else ""
    st.markdown(f'<div class="section-title">{prefix}{text}</div>', unsafe_allow_html=True)


def fmt_currency(v) -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:+,.2f}M"
    elif abs(v) >= 1_000:
        return f"${v/1_000:+,.1f}K"
    return f"${v:+,.0f}"


def render_step_bar(current_step: int, steps: list):
    dots = []
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


def render_filter_chips(row: dict):
    """Render filter dimension chips from an adjustment row (dict or pandas row)."""
    dim_labels = {
        "ENTITY_CODE": "Entity", "SOURCE_SYSTEM_CODE": "Source",
        "DEPARTMENT_CODE": "Dept", "BOOK_CODE": "Book",
        "CURRENCY_CODE": "Ccy", "TRADE_TYPOLOGY": "Typology",
        "TRADE_CODE": "Trade", "STRATEGY": "Strategy",
        "INSTRUMENT_CODE": "Instrument", "SIMULATION_NAME": "Simulation",
        "MEASURE_TYPE_CODE": "Measure",
    }
    chips = []
    for col, label in dim_labels.items():
        val = row.get(col)
        if val and str(val).strip() and str(val) != "None":
            chips.append(f'<span class="filter-chip">{label}: {val}</span>')
    if chips:
        st.markdown(f'<div class="adj-filters">{"".join(chips)}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="font-size:0.78rem;color:#9E9E9E">All records (no filters)</span>',
                    unsafe_allow_html=True)


def render_status_timeline(history_rows):
    """Render a vertical timeline of status changes.
    Expects list of dicts/rows with keys: NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT.
    """
    if not history_rows:
        st.markdown(f'<span style="font-size:0.82rem;color:{P["grey_700"]}">No history yet.</span>',
                    unsafe_allow_html=True)
        return
    html = '<div class="timeline">'
    for h in history_rows:
        status = h.get("NEW_STATUS", "?")
        color  = STATUS_COLORS.get(status, "#9E9E9E")
        icon   = STATUS_ICONS.get(status, "•")
        by     = h.get("CHANGED_BY", "system")
        at     = h.get("CHANGED_AT", "")
        if hasattr(at, "strftime"):
            at = at.strftime("%d %b %Y %H:%M")
        comment = h.get("COMMENT", "")
        html += (
            f'<div class="tl-item">'
            f'<div class="tl-status" style="color:{color}">{icon} {status}</div>'
            f'<div class="tl-meta">by {by} · {at}</div>'
            + (f'<div class="tl-comment">"{comment}"</div>' if comment else "")
            + '</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_pipeline_diagram(current_stage: int = 0):
    stages = [
        ("💾", "ADJ Header\nInsert"),
        ("⏰", "Task Polls\n(≤1 min)"),
        ("🔄", "SP_RUN_PIPELINE\nExecutes"),
        ("📊", "Dynamic Table\nRefresh"),
        ("📈", "Report\nRefresh"),
    ]
    nodes = []
    for i, (icon, label) in enumerate(stages, 1):
        state_class = "done" if i < current_stage else ("active" if i == current_stage else "")
        nodes.append(
            f'<div class="pipe-node {state_class}">'
            f'<div class="pn-icon">{icon}</div>'
            f'<div class="pn-label">{label}</div></div>')
        if i < len(stages):
            nodes.append('<div class="pipe-arrow">→</div>')
    st.markdown(f'<div class="pipeline">{"".join(nodes)}</div>', unsafe_allow_html=True)


def render_sidebar():
    """Render the branded sidebar: MUFG logo, compact nav, user at bottom."""
    from utils.snowflake_conn import current_user_name

    user = current_user_name()

    with st.sidebar:
        # ── MUFG Logo (SVG inline, white text for dark background) ──────────
        st.markdown("""
        <div style="padding:0.8rem 1rem 0.6rem;border-bottom:1px solid rgba(255,255,255,0.08)">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 80"
               style="width:100%;max-width:170px;height:auto;display:block">
            <circle cx="40" cy="40" r="34" fill="#D50032"/>
            <circle cx="40" cy="40" r="20" fill="#fff"/>
            <circle cx="40" cy="40" r="12" fill="#D50032"/>
            <text x="90" y="52" font-family="Arial,Helvetica,sans-serif"
                  font-size="38" font-weight="700" fill="#ffffff" letter-spacing="3">MUFG</text>
          </svg>
          <div style="font-size:0.67rem;font-weight:600;text-transform:uppercase;
            letter-spacing:.1em;color:rgba(255,255,255,0.4);margin-top:5px;padding-left:1px">
            Adjustment Engine
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Nav links are rendered automatically by Streamlit (stSidebarNav) here.

        # ── Spacer pushes user section to the bottom ─────────────────────────
        st.markdown(
            '<div style="flex:1;min-height:1rem"></div>',
            unsafe_allow_html=True)

        # ── User footer ───────────────────────────────────────────────────────
        st.markdown(
            f'<div class="sidebar-user-footer">'
            f'<div style="font-size:0.6rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.1em;color:rgba(255,255,255,0.38);margin-bottom:3px">'
            f'Logged in as</div>'
            f'<div style="font-size:0.82rem;font-weight:600;color:rgba(255,255,255,0.88)">'
            f'{user}</div>'
            f'</div>',
            unsafe_allow_html=True)
