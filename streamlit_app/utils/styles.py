"""
Design System — MUFG Brand
============================
Professional banking UI. Colours, CSS, SVG icons, and reusable components.

Tokens: slate-tinted neutrals, MUFG red brand, one radius scale (6/10/14),
one elevation scale (sm/md/lg). Icons are inline Lucide-style SVGs — no
external requests, so they render inside the Streamlit-in-Snowflake sandbox
(external origins like Google Fonts are blocked there).
"""
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# COLOUR PALETTE
# ──────────────────────────────────────────────────────────────────────────────

P = {
    "primary":    "#D50032",
    "primary_dk": "#A80028",
    "primary_lt": "#FF3D5A",
    "accent":     "#0F172A",
    "grey_900":   "#0F172A",
    "grey_700":   "#475569",
    "grey_400":   "#94A3B8",
    "grey_100":   "#F1F5F9",
    "white":      "#FFFFFF",
    "bg":         "#F6F7F9",
    "card":       "#FFFFFF",
    "border":     "#E2E5EA",
    "success":    "#15803D",
    "success_lt": "#F0FDF4",
    "warning":    "#B45309",
    "warning_lt": "#FFFBEB",
    "danger":     "#B91C1C",
    "danger_lt":  "#FEF2F2",
    "info":       "#1D4ED8",
    "info_lt":    "#EFF6FF",
    "purple":     "#7E22CE",
    "purple_lt":  "#FAF5FF",
}

# ──────────────────────────────────────────────────────────────────────────────
# SVG ICON SYSTEM (Lucide outline, 24px viewBox, stroke-based)
# ──────────────────────────────────────────────────────────────────────────────

_ICON_PATHS = {
    "clock":          '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    "check":          '<path d="M20 6 9 17l-5-5"/>',
    "check-circle":   '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/>',
    "x":              '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    "x-circle":       '<circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>',
    "zap":            '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
    "lock":           '<rect width="18" height="11" x="3" y="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>',
    "trash":          '<path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',
    "bar-chart":      '<line x1="12" x2="12" y1="20" y2="10"/><line x1="18" x2="18" y1="20" y2="4"/><line x1="6" x2="6" y1="20" y2="16"/>',
    "activity":       '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
    "landmark":       '<line x1="3" x2="21" y1="22" y2="22"/><line x1="6" x2="6" y1="18" y2="11"/><line x1="10" x2="10" y1="18" y2="11"/><line x1="14" x2="14" y1="18" y2="11"/><line x1="18" x2="18" y1="18" y2="11"/><polygon points="12 2 20 7 4 7"/>',
    "target":         '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
    "refresh-cw":     '<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/><path d="M8 16H3v5"/>',
    "minus-circle":   '<circle cx="12" cy="12" r="10"/><path d="M8 12h8"/>',
    "upload":         '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/>',
    "send":           '<path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/>',
    "shield-check":   '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>',
    "ban":            '<circle cx="12" cy="12" r="10"/><path d="m4.9 4.9 14.2 14.2"/>',
    "scale":          '<path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="M7 21h10"/><path d="M12 3v18"/><path d="M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/>',
    "database":       '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/>',
    "play":           '<polygon points="6 3 20 12 6 21 6 3"/>',
    "table":          '<path d="M12 3v18"/><rect width="18" height="18" x="3" y="3" rx="2"/><path d="M3 9h18"/><path d="M3 15h18"/>',
    "line-chart":     '<path d="M3 3v16a2 2 0 0 0 2 2h16"/><path d="m19 9-5 5-4-4-3 3"/>',
    "user":           '<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
    "alert-triangle": '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    "info":           '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    "clipboard":      '<rect width="8" height="4" x="8" y="2" rx="1" ry="1"/><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><path d="M12 11h4"/><path d="M12 16h4"/><path d="M8 11h.01"/><path d="M8 16h.01"/>',
    "home":           '<path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    "file-text":      '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M10 9H8"/><path d="M16 13H8"/><path d="M16 17H8"/>',
    "eye":            '<path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/>',
    "inbox":          '<polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
    "chevron-right":  '<path d="m9 18 6-6-6-6"/>',
    "arrow-right":    '<path d="M5 12h14"/><path d="m12 5 7 7-7 7"/>',
    "sliders":        '<line x1="4" x2="4" y1="21" y2="14"/><line x1="4" x2="4" y1="10" y2="3"/><line x1="12" x2="12" y1="21" y2="12"/><line x1="12" x2="12" y1="8" y2="3"/><line x1="20" x2="20" y1="21" y2="16"/><line x1="20" x2="20" y1="12" y2="3"/><line x1="2" x2="6" y1="14" y2="14"/><line x1="10" x2="14" y1="8" y2="8"/><line x1="18" x2="22" y1="16" y2="16"/>',
    "settings":       '<path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/>',
    "search":         '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>',
    "calendar":       '<path d="M8 2v4"/><path d="M16 2v4"/><rect width="18" height="18" x="3" y="4" rx="2"/><path d="M3 10h18"/>',
    "layers":         '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 12.18-9.17 4.16a2 2 0 0 1-1.66 0L2 12.18"/><path d="m22 17.18-9.17 4.16a2 2 0 0 1-1.66 0L2 17.18"/>',
    "timer":          '<line x1="10" x2="14" y1="2" y2="2"/><line x1="12" x2="15" y1="14" y2="11"/><circle cx="12" cy="14" r="8"/>',
    "undo":           '<path d="M3 7v6h6"/><path d="M21 17a9 9 0 0 0-9-9 9 9 0 0 0-6 2.3L3 13"/>',
    "list":           '<line x1="8" x2="21" y1="6" y2="6"/><line x1="8" x2="21" y1="12" y2="12"/><line x1="8" x2="21" y1="18" y2="18"/><line x1="3" x2="3.01" y1="6" y2="6"/><line x1="3" x2="3.01" y1="12" y2="12"/><line x1="3" x2="3.01" y1="18" y2="18"/>',
}


def icon(name: str, size: int = 14, color: str = "currentColor",
         stroke: float = 2, valign: str = "-2px") -> str:
    """Inline Lucide-style SVG icon. Returns '' for unknown names."""
    path = _ICON_PATHS.get(name)
    if not path:
        return ""
    return (f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="{color}" stroke-width="{stroke}" stroke-linecap="round" '
            f'stroke-linejoin="round" style="vertical-align:{valign};flex-shrink:0">'
            f'{path}</svg>')


# ──────────────────────────────────────────────────────────────────────────────
# STATUS MAPPING
# ──────────────────────────────────────────────────────────────────────────────

STATUS_COLORS = {
    "Pending":              "#B45309",
    "Pending Approval":     "#1D4ED8",
    "Approved":             "#0F766E",
    "Running":              "#1D4ED8",
    "Processed":            "#15803D",
    "Failed":               "#DC2626",
    "Rejected":             "#B91C1C",
    "Rejected - SignedOff": "#7E22CE",
    "Deleted":              "#64748B",
}

# Values are icon() names (previously emoji)
STATUS_ICONS = {
    "Pending":              "clock",
    "Pending Approval":     "clipboard",
    "Approved":             "shield-check",
    "Running":              "zap",
    "Processed":            "check-circle",
    "Failed":               "x-circle",
    "Rejected":             "ban",
    "Rejected - SignedOff": "lock",
    "Deleted":              "trash",
}

SCOPE_CONFIG = {
    "VaR":         {"icon": "bar-chart",  "color": "#D50032", "bg": "#FFF0F3", "label": "VaR"},
    "Stress":      {"icon": "activity",   "color": "#1D4ED8", "bg": "#EFF6FF", "label": "Stress"},
    "FRTB":        {"icon": "landmark",   "color": "#15803D", "bg": "#F0FDF4", "label": "FRTB"},
    "Sensitivity": {"icon": "target",     "color": "#B45309", "bg": "#FFFBEB", "label": "Sensitivity"},
}

TYPE_CONFIG = {
    "Flatten": {"icon": "minus-circle", "desc": "Zero out matching positions", "formula": "new = original × 0"},
    "Scale":   {"icon": "bar-chart",    "desc": "Multiply by a scale factor", "formula": "new = original × sf"},
    "Roll":    {"icon": "refresh-cw",   "desc": "Roll prior COB's adjusted state forward", "formula": "new = (prior + prior_adj) × sf"},
}

# ── Lifecycle stage colours (used by tracker board and lifecycle bar) ───────

STAGE_CONFIG = {
    "Submitted":        {"color": "#B45309", "icon": "send",         "bg": "#FFFBEB"},
    "Pending Approval": {"color": "#1D4ED8", "icon": "clipboard",    "bg": "#EFF6FF"},
    "Approved":         {"color": "#0F766E", "icon": "shield-check", "bg": "#F0FDFA"},
    "Processing":       {"color": "#1D4ED8", "icon": "zap",          "bg": "#EFF6FF"},
    "PBI Queued":       {"color": "#7E22CE", "icon": "clock",        "bg": "#FAF5FF"},
    "PBI Refreshing":   {"color": "#7E22CE", "icon": "refresh-cw",   "bg": "#FAF5FF"},
    "Reports Ready":    {"color": "#15803D", "icon": "check-circle", "bg": "#F0FDF4"},
    "Failed":           {"color": "#DC2626", "icon": "x-circle",     "bg": "#FEF2F2"},
    "Rejected":         {"color": "#B91C1C", "icon": "ban",          "bg": "#FEF2F2"},
}

# ── Adjustment Category — the first selection in the wizard ──────────────────

CATEGORY_CONFIG = {
    "Scaling Adjustment": {
        "icon": "scale", "color": "#D50032", "bg": "#FFF0F3",
        "desc": "Scale, flatten or roll fact table data (config-driven)",
    },
    "Direct Adjustment": {
        "icon": "upload", "color": "#7E22CE", "bg": "#FAF5FF",
        "desc": "Upload exact adjustment values for a chosen scope (CSV)",
    },
    "Entity Roll": {
        "icon": "refresh-cw", "color": "#B45309", "bg": "#FFFBEB",
        "desc": "Full entity roll — copies all data from a source COB (approval required)",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS
# ──────────────────────────────────────────────────────────────────────────────

def inject_css():
    st.markdown(f"""
    <style>
    :root {{
        --brand:      {P["primary"]};
        --brand-dk:   {P["primary_dk"]};
        --ink:        {P["grey_900"]};
        --ink-2:      {P["grey_700"]};
        --ink-3:      {P["grey_400"]};
        --bg:         {P["bg"]};
        --card:       {P["card"]};
        --border:     {P["border"]};
        --r-sm: 6px;  --r-md: 10px;  --r-lg: 14px;
        --sh-sm: 0 1px 2px rgba(15,23,42,.05);
        --sh-md: 0 2px 8px rgba(15,23,42,.06);
        --sh-lg: 0 8px 24px rgba(15,23,42,.10);
    }}

    /* System font stack — external fonts don't load inside the SiS sandbox */
    html, body, [class*="css"] {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
                     'Helvetica Neue', Arial, sans-serif !important;
        -webkit-font-smoothing: antialiased;
    }}
    h1, h2, h3 {{ letter-spacing: -0.01em; }}
    [data-testid="stAppViewContainer"] {{ background: var(--bg) !important; }}
    [data-testid="stMainBlockContainer"] {{ padding-top: 1.5rem; padding-bottom: 3rem; }}
    [data-testid="stVerticalBlock"] {{ gap: 0.75rem; }}

    @media (prefers-reduced-motion: reduce) {{
        * {{ transition: none !important; animation: none !important; }}
    }}

    /* ── Sidebar — MUFG brand dark ─────────────────────────────────────── */
    [data-testid="stSidebar"] {{
        background: #26272B !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] > div:first-child {{
        background: #26272B !important;
        overflow-y: auto !important;
        height: 100vh !important;
        display: flex !important;
        flex-direction: column !important;
    }}
    [data-testid="stSidebar"] * {{ color: #E2E5EA !important; }}
    [data-testid="stSidebar"] hr {{ border-color: rgba(255,255,255,0.10) !important; }}

    /* Logo area red top-stripe */
    [data-testid="stSidebar"] > div:first-child::before {{
        content: "";
        display: block;
        height: 3px;
        background: var(--brand);
        flex-shrink: 0;
    }}

    /* Compact sidebar navigation — all pages visible with no scroll */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] {{
        padding-top: 0.3rem !important;
        flex-shrink: 0 !important;
        max-height: none !important;
        overflow: visible !important;
        height: auto !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] ul {{
        padding-bottom: 0.2rem !important;
        max-height: none !important;
        overflow: visible !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] li {{
        margin: 1px 8px !important;
        padding: 0 !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a {{
        padding: 0.32rem 0.7rem !important;
        font-size: 0.83rem !important;
        line-height: 1.25 !important;
        min-height: unset !important;
        border-radius: var(--r-sm) !important;
        transition: background .15s ease-out !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {{
        background: rgba(255,255,255,.06) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-selected="true"] {{
        background: rgba(213,0,50,.28) !important;
        box-shadow: inset 3px 0 0 var(--brand) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a span {{
        font-size: 0.83rem !important;
        white-space: nowrap !important;
    }}
    /* Rename "app" → "Home" for the main page nav link */
    [data-testid="stSidebarNav"] li:first-child a span {{
        font-size: 0 !important;
    }}
    [data-testid="stSidebarNav"] li:first-child a span::after {{
        content: "Home";
        font-size: 0.83rem !important;
    }}
    [data-testid="stSidebarNav"] {{
        flex: 0 0 auto !important;
    }}

    /* Sidebar user footer */
    .sidebar-user-footer {{
        border-top: 1px solid rgba(255,255,255,0.10);
        padding: 0.65rem 1rem;
        margin-top: 0.75rem;
        background: #1F2023;
        flex-shrink: 0 !important;
        display: flex; align-items: center; gap: 9px;
    }}

    /* Cards */
    .mcard {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: var(--r-md);
        padding: 1.2rem 1.4rem;
        margin-bottom: 0.75rem;
        box-shadow: var(--sh-sm);
        transition: box-shadow .15s ease-out, transform .15s ease-out;
    }}
    .mcard:hover {{ box-shadow: var(--sh-lg); }}

    /* KPI cards */
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(155px, 1fr));
        gap: 14px;
        margin-bottom: 1.25rem;
    }}
    .kpi-card {{
        position: relative;
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: var(--r-md);
        padding: 1rem 1.1rem 1rem 1.25rem;
        box-shadow: var(--sh-sm);
        overflow: hidden;
        transition: box-shadow .15s ease-out;
    }}
    .kpi-card:hover {{ box-shadow: var(--sh-md); }}
    .kpi-card::before {{
        content: ""; position: absolute; left: 0; top: 0; bottom: 0;
        width: 3px; background: var(--border);
    }}
    .kpi-card .kpi-label {{
        font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: .07em; color: var(--ink-2); margin-bottom: 0.35rem;
    }}
    .kpi-card .kpi-value {{
        font-size: 1.8rem; font-weight: 700; color: var(--ink); line-height: 1.05;
        font-variant-numeric: tabular-nums;
    }}
    .kpi-card .kpi-sub {{ font-size: 0.72rem; color: var(--ink-2); margin-top: 0.3rem; }}
    .kpi-card.kpi-primary::before {{ background: {P["primary"]}; }}
    .kpi-card.kpi-warning::before {{ background: {P["warning"]}; }}
    .kpi-card.kpi-success::before {{ background: {P["success"]}; }}
    .kpi-card.kpi-info::before    {{ background: {P["info"]}; }}
    .kpi-card.kpi-purple::before  {{ background: {P["purple"]}; }}
    .kpi-card.kpi-danger::before  {{ background: {P["danger"]}; }}

    /* Status badges */
    .status-badge {{
        display: inline-flex; align-items: center; gap: 5px;
        padding: 2px 10px; border-radius: 999px;
        font-size: 0.72rem; font-weight: 600; letter-spacing: .03em;
        white-space: nowrap; border: 1px solid transparent;
    }}

    /* Adjustment cards */
    .adj-card {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: var(--r-md); padding: 1rem 1.2rem; margin-bottom: 0.6rem;
        box-shadow: var(--sh-sm);
        transition: box-shadow .15s ease-out;
    }}
    .adj-card:hover {{ box-shadow: var(--sh-md); }}
    .adj-card-header {{
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem;
    }}
    .adj-id {{ font-size: 0.8rem; font-weight: 700; color: var(--ink-2); letter-spacing: .04em;
               font-variant-numeric: tabular-nums; }}
    .adj-title {{ font-size: 0.95rem; font-weight: 600; color: var(--ink); }}
    .adj-meta {{ font-size: 0.78rem; color: var(--ink-2); }}
    .adj-filters {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 0.5rem; }}
    .filter-chip {{
        background: {P["grey_100"]}; border: 1px solid var(--border);
        border-radius: 999px; padding: 1px 9px;
        font-size: 0.71rem; color: var(--ink-2); font-weight: 500;
    }}

    /* Overlap warning */
    .overlap-box {{
        background: {P["warning_lt"]}; border: 1px solid #FDE68A;
        border-left: 4px solid {P["warning"]}; border-radius: var(--r-sm);
        padding: 1rem 1.2rem; margin: 0.75rem 0;
    }}
    .overlap-box h4 {{ color: {P["warning"]}; margin: 0 0 0.5rem 0; font-size: 0.9rem; }}

    /* Timeline */
    .timeline {{ position: relative; padding-left: 24px; }}
    .timeline::before {{
        content: ""; position: absolute; left: 8px; top: 0; bottom: 0;
        width: 2px; background: var(--border);
    }}
    .tl-item {{
        position: relative; margin-bottom: 1rem; padding: 0.6rem 0.8rem;
        background: var(--card); border: 1px solid var(--border); border-radius: var(--r-sm);
    }}
    .tl-item::before {{
        content: ""; position: absolute; left: -20px; top: 14px;
        width: 10px; height: 10px; border-radius: 50%;
        background: var(--ink-3); border: 2px solid var(--card);
        box-shadow: 0 0 0 2px var(--border);
    }}
    .tl-status {{ font-size: 0.78rem; font-weight: 700; text-transform: uppercase;
                  display: inline-flex; align-items: center; gap: 5px; }}
    .tl-meta {{ font-size: 0.73rem; color: var(--ink-2); margin-top: 2px; }}
    .tl-comment {{ font-size: 0.8rem; color: var(--ink); margin-top: 4px; font-style: italic; }}

    /* Queue items */
    .queue-item {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: var(--r-sm); padding: 0.75rem 1rem; margin-bottom: 0.5rem;
    }}
    .queue-item.running {{ border-left: 3px solid {P["info"]}; }}
    .queue-item.pending {{ border-left: 3px solid {P["warning"]}; }}
    .queue-item.completed {{ border-left: 3px solid {P["success"]}; opacity: 0.85; }}
    .queue-item.failed {{ border-left: 3px solid {P["danger"]}; }}

    /* Pipeline diagram */
    .pipeline {{
        display: flex; align-items: center; gap: 0; overflow-x: auto;
        padding: 1rem; background: {P["grey_100"]}; border-radius: var(--r-md); margin: 1rem 0;
    }}
    .pipe-node {{
        background: var(--card); border: 1.5px solid var(--border);
        border-radius: var(--r-sm); padding: 0.6rem 0.9rem; text-align: center;
        min-width: 100px; flex-shrink: 0;
    }}
    .pipe-node.active {{ border-color: {P["info"]}; background: {P["info_lt"]}; }}
    .pipe-node.done {{ border-color: {P["success"]}; background: {P["success_lt"]}; }}
    .pipe-node .pn-icon {{ display: flex; justify-content: center; color: var(--ink-2); }}
    .pipe-node.active .pn-icon {{ color: {P["info"]}; }}
    .pipe-node.done .pn-icon {{ color: {P["success"]}; }}
    .pipe-node .pn-label {{ font-size: 0.7rem; font-weight: 600; margin-top: 5px; color: var(--ink-2); }}
    .pipe-arrow {{ color: var(--ink-3); display: flex; align-items: center; padding: 0 4px; flex-shrink: 0; }}

    /* Step indicator */
    .step-bar {{ display: flex; align-items: center; gap: 0; margin-bottom: 1.5rem; }}
    .step {{ display: flex; align-items: center; gap: 6px; flex: 1; }}
    .step-dot {{
        width: 26px; height: 26px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        font-size: 0.75rem; font-weight: 700; flex-shrink: 0;
        transition: background .15s ease-out;
    }}
    .step-dot.done    {{ background: {P["success"]}; color: white; }}
    .step-dot.active  {{ background: {P["primary"]}; color: white;
                         box-shadow: 0 0 0 3px rgba(213,0,50,.18); }}
    .step-dot.pending {{ background: {P["grey_100"]}; color: var(--ink-2); border: 2px solid var(--ink-3); }}
    .step-label {{ font-size: 0.78rem; font-weight: 600; white-space: nowrap; }}
    .step-label.done    {{ color: {P["success"]}; }}
    .step-label.active  {{ color: {P["primary"]}; }}
    .step-label.pending {{ color: var(--ink-2); }}
    .step-line {{ flex: 1; height: 2px; background: var(--border); margin: 0 6px; }}
    .step-line.done {{ background: {P["success"]}; }}

    /* Section headers */
    .section-title {{
        font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: .08em; color: var(--ink-2); margin: 1rem 0 0.6rem 0;
        display: flex; align-items: center; gap: 7px;
    }}
    .section-title::after {{ content: ""; flex: 1; height: 1px; background: var(--border); }}

    /* Tags */
    .tag {{
        display: inline-block; background: {P["info_lt"]}; color: {P["info"]};
        border-radius: 999px; padding: 1px 8px; font-size: 0.7rem; font-weight: 600;
    }}
    .tag.recurring {{ background: {P["purple_lt"]}; color: {P["purple"]}; }}

    /* Native widget polish */
    .stButton>button {{
        border-radius: var(--r-sm) !important; font-weight: 600 !important;
        font-size: 0.85rem !important;
        border: 1px solid var(--border) !important;
        box-shadow: var(--sh-sm) !important;
        transition: background .15s ease-out, box-shadow .15s ease-out,
                    border-color .15s ease-out !important;
    }}
    .stButton>button:hover {{
        border-color: var(--ink-3) !important;
        box-shadow: var(--sh-md) !important;
    }}
    .stButton>button:focus-visible {{
        outline: 2px solid {P["info"]} !important; outline-offset: 2px !important;
    }}
    .stButton>button[kind="primary"], .stButton>button[data-testid="stBaseButton-primary"] {{
        background: var(--brand) !important; border-color: var(--brand) !important;
        color: white !important;
    }}
    .stButton>button[kind="primary"]:hover, .stButton>button[data-testid="stBaseButton-primary"]:hover {{
        background: var(--brand-dk) !important; border-color: var(--brand-dk) !important;
    }}
    [data-testid="stDataFrame"] {{
        border-radius: var(--r-sm); overflow: hidden;
        border: 1px solid var(--border);
    }}
    div[data-testid="stExpander"] {{
        border: 1px solid var(--border); border-radius: var(--r-md);
        overflow: hidden; background: var(--card); box-shadow: var(--sh-sm);
    }}
    div[data-testid="stExpander"] summary {{ font-weight: 600; }}
    .stTabs [data-baseweb="tab"] {{
        font-weight: 600; font-size: 0.86rem; color: var(--ink-2);
    }}
    .stTabs [data-baseweb="tab"][aria-selected="true"] {{ color: var(--brand); }}
    .stTabs [data-baseweb="tab-highlight"] {{ background-color: var(--brand); }}
    [data-testid="stTextInput"] input, [data-testid="stNumberInput"] input,
    [data-testid="stTextArea"] textarea {{
        border-radius: var(--r-sm) !important;
    }}
    [data-baseweb="select"] > div {{ border-radius: var(--r-sm) !important; }}

    /* Thin scrollbars */
    ::-webkit-scrollbar {{ height: 8px; width: 8px; }}
    ::-webkit-scrollbar-thumb {{ background: var(--ink-3); border-radius: 4px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}

    /* Segmented pill selectors — st.pills, and horizontal st.radio fallback */
    [data-testid="stPills"] button {{
        border-radius: 999px !important;
        font-weight: 600 !important;
        font-size: 0.82rem !important;
    }}
    [data-testid="stPills"] button[data-testid="stBaseButton-pillsActive"] {{
        background: var(--brand) !important;
        border-color: var(--brand) !important;
        color: #fff !important;
    }}
    .stRadio [role="radiogroup"] {{ gap: 6px; flex-wrap: wrap; }}
    .stRadio [role="radiogroup"] label {{
        border: 1px solid var(--border);
        border-radius: 999px;
        padding: 3px 14px 3px 10px;
        background: var(--card);
        transition: border-color .15s ease-out, background .15s ease-out;
    }}
    .stRadio [role="radiogroup"] label:hover {{ border-color: var(--ink-3); }}
    .stRadio [role="radiogroup"] label:has(input:checked) {{
        border-color: var(--brand);
        background: #FFF0F3;
    }}
    .stRadio [role="radiogroup"] label:has(input:checked) p {{ color: var(--brand) !important; font-weight: 700; }}

    /* Order-ticket summary panel (New Adjustment) */
    .ticket {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: var(--r-md); box-shadow: var(--sh-sm);
        position: relative; overflow: hidden; margin-bottom: 0.75rem;
    }}
    .ticket::before {{
        content: ""; position: absolute; left: 0; top: 0; bottom: 0;
        width: 3px; background: var(--brand);
    }}
    .ticket .t-head {{
        padding: 0.7rem 1.1rem; border-bottom: 1px solid var(--border);
        display: flex; align-items: center; gap: 8px;
        font-weight: 700; font-size: 0.85rem;
    }}
    .ticket .t-body {{ padding: 0.8rem 1.1rem; }}
    .ticket .kv {{ display: flex; justify-content: space-between; gap: 12px;
                   font-size: 0.8rem; padding: 3px 0; }}
    .ticket .kv .k {{ color: var(--ink-2); white-space: nowrap; }}
    .ticket .kv .v {{ font-weight: 600; font-variant-numeric: tabular-nums;
                      text-align: right; }}
    .ticket .t-imp {{
        margin-top: 0.8rem; background: #F8FAFC;
        border: 1px solid var(--border); border-radius: var(--r-sm);
        padding: 0.6rem 0.85rem;
    }}
    .ticket .t-warn {{
        margin-top: 0.8rem; background: {P["warning_lt"]};
        border: 1px solid #FDE68A; border-radius: var(--r-sm);
        padding: 0.55rem 0.85rem; font-size: 0.76rem; color: {P["warning"]};
    }}
    .ticket .t-missing {{
        margin-top: 0.8rem; background: #F8FAFC;
        border: 1px dashed var(--border); border-radius: var(--r-sm);
        padding: 0.55rem 0.85rem; font-size: 0.76rem; color: var(--ink-2);
    }}

    /* Lifecycle progress bar */
    .lifecycle-bar {{
        display: flex;
        align-items: flex-start;
        gap: 0;
        margin: 0.6rem 0 0.8rem 0;
        padding: 0.5rem 0.8rem;
        background: {P["grey_100"]};
        border-radius: var(--r-sm);
        overflow-x: auto;
    }}
    .lc-stage {{
        display: flex;
        flex-direction: column;
        align-items: center;
        min-width: 70px;
        flex-shrink: 0;
    }}
    .lc-dot {{
        width: 22px;
        height: 22px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
    }}
    .lc-dot.completed {{
        background: {P["success"]};
        color: white;
    }}
    .lc-dot.current {{
        background: {P["info"]};
        color: white;
        box-shadow: 0 0 0 3px rgba(29,78,216,0.22);
    }}
    .lc-dot.failed {{
        background: #DC2626;
        color: white;
    }}
    .lc-dot.upcoming {{
        background: {P["grey_100"]};
        color: var(--ink-3);
        border: 2px solid var(--border);
    }}
    .lc-label {{
        font-size: 0.62rem;
        font-weight: 600;
        margin-top: 3px;
        text-align: center;
        white-space: nowrap;
    }}
    .lc-time {{
        font-size: 0.58rem;
        color: var(--ink-3);
        margin-top: 1px;
        text-align: center;
        font-variant-numeric: tabular-nums;
    }}
    .lc-connector {{
        width: 28px;
        height: 2px;
        margin-top: 10px;
        flex-shrink: 0;
    }}
    .lc-connector.completed {{ background: {P["success"]}; }}
    .lc-connector.upcoming  {{ background: var(--border); }}
    .lc-connector.failed    {{ background: #DC2626; }}

    /* Tracker board */
    .tracker-board {{
        display: flex;
        gap: 10px;
        overflow-x: auto;
        padding: 0.5rem 0;
        margin-bottom: 1rem;
    }}
    .board-col {{
        flex: 1;
        min-width: 140px;
        max-width: 220px;
        background: {P["grey_100"]};
        border-radius: var(--r-md);
        padding: 0.6rem;
        border-top: 3px solid var(--border);
    }}
    .board-col-header {{
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .05em;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }}
    .board-col-count {{
        background: white;
        border-radius: 999px;
        padding: 1px 7px;
        font-size: 0.68rem;
        font-weight: 700;
        font-variant-numeric: tabular-nums;
    }}
    .board-item {{
        background: white;
        border: 1px solid var(--border);
        border-radius: var(--r-sm);
        padding: 0.4rem 0.6rem;
        margin-bottom: 0.4rem;
        font-size: 0.72rem;
        box-shadow: var(--sh-sm);
    }}
    .board-item .bi-scope {{
        font-weight: 600;
        font-size: 0.68rem;
    }}
    .board-item .bi-detail {{
        color: var(--ink-2);
        font-size: 0.65rem;
        margin-top: 2px;
    }}
    </style>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# REUSABLE COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#64748B")
    icon_name = STATUS_ICONS.get(status, "")
    bg = color + "14"
    svg = icon(icon_name, size=12, color=color) if icon_name else ""
    return (f'<span class="status-badge" style="background:{bg};color:{color};'
            f'border-color:{color}33">{svg}{status}</span>')


def kpi_card(label: str, value, sub: str = "", variant: str = "primary") -> str:
    return (f'<div class="kpi-card kpi-{variant}">'
            f'<div class="kpi-label">{label}</div>'
            f'<div class="kpi-value">{value}</div>'
            f'<div class="kpi-sub">{sub}</div>'
            f'</div>')


def section_title(text: str, icon_name: str = ""):
    """Section header. `icon_name` accepts an icon() name; anything else
    (legacy emoji) is rendered as-is."""
    prefix = ""
    if icon_name:
        svg = icon(icon_name, size=13)
        prefix = f"{svg} " if svg else f"{icon_name} "
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


def fmt_adj_id(dimension_adj_id, prefix: str = "#") -> str:
    """Business-facing adjustment identifier.

    Shows DIMENSION_ADJ_ID (the numeric DIMENSION.ADJUSTMENT id) — never the
    internal ADJ_ID hash. Returns "Pending" while the adjustment has not been
    processed yet (DIMENSION_ADJ_ID is set by SP_PROCESS_ADJUSTMENT, so it is
    NULL for Pending/Running/Failed rows)."""
    import pandas as pd
    if dimension_adj_id is None or (isinstance(dimension_adj_id, float) and pd.isna(dimension_adj_id)):
        return "Pending"
    try:
        return f"{prefix}{int(dimension_adj_id)}"
    except (TypeError, ValueError):
        s = str(dimension_adj_id).strip()
        return f"{prefix}{s}" if s else "Pending"


def render_step_bar(current_step: int, steps: list):
    dots = []
    for i, label in enumerate(steps, 1):
        state = "done" if i < current_step else ("active" if i == current_step else "pending")
        glyph = icon("check", size=13, color="white", valign="0") if state == "done" else str(i)
        dots.append(
            f'<div class="step">'
            f'<div class="step-dot {state}">{glyph}</div>'
            f'<span class="step-label {state}">{label}</span>'
            f'</div>')
        if i < len(steps):
            line_state = "done" if i < current_step else ""
            dots.append(f'<div class="step-line {line_state}"></div>')
    st.markdown(f'<div class="step-bar">{"".join(dots)}</div>', unsafe_allow_html=True)


def render_filter_chips(row: dict):
    """Render filter dimension chips from an adjustment row (dict or pandas row).

    Covers every filter dimension ADJ_HEADER captures, in form order. The source
    view must expose these columns (VW_MY_WORK / VW_APPROVAL_QUEUE) or the value
    reads as missing and the chip is skipped."""
    dim_labels = {
        "ENTITY_CODE": "Entity", "SOURCE_SYSTEM_CODE": "Source",
        "DEPARTMENT_CODE": "Dept", "BOOK_CODE": "Book",
        "CURRENCY_CODE": "Ccy", "TRADE_TYPOLOGY": "Typology",
        "TRADE_CODE": "Trade", "STRATEGY": "Strategy",
        "INSTRUMENT_CODE": "Instrument", "SIMULATION_NAME": "Simulation",
        "SIMULATION_SOURCE": "Sim Source", "MEASURE_TYPE_CODE": "Measure",
        "TRADER_CODE": "Trader", "VAR_COMPONENT_ID": "VaR Comp",
        "VAR_SUB_COMPONENT_ID": "VaR Sub-Comp", "GUARANTEED_ENTITY": "Guaranteed Entity",
        "REGION_KEY": "Region", "SCENARIO_DATE_ID": "Scenario Date",
        "TENOR_CODE": "Tenor", "UNDERLYING_TENOR_CODE": "Underlying Tenor",
        "CURVE_CODE": "Curve", "DAY_TYPE": "Day Type",
        "PRODUCT_CATEGORY_ATTRIBUTES": "Product Cat", "BATCH_REGION_AREA": "Region Area",
        "MUREX_FAMILY": "Murex Family", "MUREX_GROUP": "Murex Group",
    }

    def _fmt(v):
        # NUMBER columns arrive from pandas as float (e.g. 5.0) when the column is
        # nullable — show integral values without the trailing ".0".
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v).strip()

    chips = []
    for col, label in dim_labels.items():
        val = row.get(col)
        if val is None:
            continue
        text = _fmt(val)
        if text and text.lower() != "none" and text.lower() != "nan":
            chips.append(f'<span class="filter-chip">{label}: {text}</span>')
    if chips:
        st.markdown(f'<div class="adj-filters">{"".join(chips)}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="font-size:0.78rem;color:#94A3B8">All records (no filters)</span>',
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
        color  = STATUS_COLORS.get(status, "#64748B")
        svg    = icon(STATUS_ICONS.get(status, ""), size=13, color=color)
        by     = h.get("CHANGED_BY", "system")
        at     = h.get("CHANGED_AT", "")
        if hasattr(at, "strftime"):
            at = at.strftime("%d %b %Y %H:%M")
        comment = h.get("COMMENT", "")
        html += (
            f'<div class="tl-item">'
            f'<div class="tl-status" style="color:{color}">{svg} {status}</div>'
            f'<div class="tl-meta">by {by} · {at}</div>'
            + (f'<div class="tl-comment">"{comment}"</div>' if comment else "")
            + '</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_pipeline_diagram(current_stage: int = 0):
    stages = [
        ("database",   "ADJ Header\nInsert"),
        ("timer",      "Task Polls\n(≤1 min)"),
        ("play",       "SP_RUN_PIPELINE\nExecutes"),
        ("table",      "Dynamic Table\nRefresh"),
        ("line-chart", "Report\nRefresh"),
    ]
    nodes = []
    for i, (icon_name, label) in enumerate(stages, 1):
        state_class = "done" if i < current_stage else ("active" if i == current_stage else "")
        nodes.append(
            f'<div class="pipe-node {state_class}">'
            f'<div class="pn-icon">{icon(icon_name, size=18, valign="0")}</div>'
            f'<div class="pn-label">{label}</div></div>')
        if i < len(stages):
            nodes.append(f'<div class="pipe-arrow">{icon("chevron-right", size=16, valign="0")}</div>')
    st.markdown(f'<div class="pipeline">{"".join(nodes)}</div>', unsafe_allow_html=True)


def render_lifecycle_bar(track_row: dict):
    """Render a compact horizontal lifecycle progress bar for one adjustment.

    Uses CURRENT_STAGE from VW_ADJUSTMENT_TRACK to drive the state (not timestamps).
    Adapts the stage list: approval stages only appear if the adjustment went
    through the approval flow.
    """
    import pandas as _pd

    def _fmt_ts(val):
        if val is None or str(val) in ("NaT", "None", ""):
            return ""
        if hasattr(val, "strftime"):
            try:
                return val.strftime("%H:%M")
            except Exception:
                return ""
        return ""

    def _is_set(val):
        """True if value is a real timestamp (not None/NaT)."""
        if val is None:
            return False
        if isinstance(val, float) and _pd.isna(val):
            return False
        if hasattr(_pd, "NaT") and val is _pd.NaT:
            return False
        if str(val) in ("NaT", "None", ""):
            return False
        return True

    current_stage = str(track_row.get("CURRENT_STAGE", "") or "")
    is_failed = current_stage in ("Failed", "Rejected")

    # Detect whether this adjustment used the approval flow
    has_approval = (
        _is_set(track_row.get("APPROVAL_REQUESTED_AT"))
        or _is_set(track_row.get("APPROVED_AT"))
        or current_stage in ("Pending Approval", "Approved")
    )

    # Build adaptive stage list: (label, timestamp, stage_keys_that_map_here)
    stages = [("Submitted", track_row.get("SUBMITTED_AT"), ["Submitted"])]
    if has_approval:
        stages.append(("Pending Approval", track_row.get("APPROVAL_REQUESTED_AT"),
                        ["Pending Approval"]))
        stages.append(("Approved", track_row.get("APPROVED_AT"),
                        ["Approved"]))
    stages.append(("Processing", track_row.get("PROCESSING_STARTED_AT"),
                    ["Processing"]))
    stages.append(("PBI Refresh",
                    track_row.get("PBI_QUEUED_AT") if _is_set(track_row.get("PBI_QUEUED_AT"))
                    else track_row.get("PBI_STARTED_AT"),
                    ["PBI Queued", "PBI Refreshing"]))
    stages.append(("Reports Ready", track_row.get("PBI_COMPLETED_AT"),
                    ["Reports Ready"]))

    # Find which stage index is the current one
    current_idx = None
    for idx, (_, _, keys) in enumerate(stages):
        if current_stage in keys:
            current_idx = idx
            break
    # Fallback: if CURRENT_STAGE doesn't match, find first stage without timestamp
    if current_idx is None and not is_failed:
        for idx, (_, ts, _) in enumerate(stages):
            if not _is_set(ts):
                current_idx = idx
                break
    if current_idx is None:
        current_idx = len(stages) - 1

    # Render each stage
    html_parts = []
    for i, (label, ts, _keys) in enumerate(stages):
        ts_str = _fmt_ts(ts) if _is_set(ts) else ""

        if is_failed and i >= current_idx:
            if i == current_idx:
                dot_class = "failed"
                label_color = "#DC2626"
                conn_class = "failed"
            else:
                dot_class = "upcoming"
                label_color = "#94A3B8"
                conn_class = "upcoming"
        elif i < current_idx:
            dot_class = "completed"
            label_color = P["success"]
            conn_class = "completed"
        elif i == current_idx:
            dot_class = "current"
            label_color = P["info"]
            conn_class = "upcoming"
        else:
            dot_class = "upcoming"
            label_color = "#94A3B8"
            conn_class = "upcoming"

        if dot_class == "completed":
            glyph = icon("check", size=12, color="white", valign="0")
        elif dot_class == "failed":
            glyph = icon("x", size=12, color="white", valign="0")
        else:
            glyph = ""
        html_parts.append(
            f'<div class="lc-stage">'
            f'<div class="lc-dot {dot_class}">{glyph}</div>'
            f'<div class="lc-label" style="color:{label_color}">{label}</div>'
            f'<div class="lc-time">{ts_str}</div>'
            f'</div>')
        if i < len(stages) - 1:
            html_parts.append(f'<div class="lc-connector {conn_class}"></div>')

    st.markdown(
        f'<div class="lifecycle-bar">{"".join(html_parts)}</div>',
        unsafe_allow_html=True)


def render_sidebar():
    """Render the branded sidebar: MUFG logo, compact nav, user at bottom."""
    from utils.snowflake_conn import current_user_name

    user = current_user_name()

    with st.sidebar:
        # ── MUFG Logo (SVG inline, white text for dark background) ──────────
        st.markdown("""
        <div style="padding:0.8rem 1rem 0.6rem;border-bottom:1px solid rgba(255,255,255,0.08);pointer-events:none">
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
            f'{icon("user", size=16, color="rgba(255,255,255,0.5)", valign="0")}'
            f'<div>'
            f'<div style="font-size:0.6rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.1em;color:rgba(255,255,255,0.38);margin-bottom:2px">'
            f'Logged in as</div>'
            f'<div style="font-size:0.82rem;font-weight:600;color:rgba(255,255,255,0.88)">'
            f'{user}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True)
