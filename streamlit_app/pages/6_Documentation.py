"""
Documentation — Full Process Guide
=====================================
Comprehensive documentation of the Adjustment Engine with interactive
diagrams, architecture overview, and operational guides.
"""
import streamlit as st
import textwrap


def _html(content: str) -> None:
    """Render an HTML block via st.markdown, stripping all line indentation.

    Removes leading whitespace from every line so that no line ever starts
    with 4+ spaces, which Markdown would treat as a code block.
    HTML renders identically regardless of source indentation.
    """
    flat = "\n".join(line.lstrip() for line in content.splitlines()).strip()
    st.markdown(flat, unsafe_allow_html=True)

st.set_page_config(
    page_title="Documentation · MUFG",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.styles import inject_css, render_sidebar, section_title, P, SCOPE_CONFIG, TYPE_CONFIG, STATUS_COLORS, STATUS_ICONS

inject_css()
render_sidebar()

st.markdown("## 📖 Documentation")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Comprehensive guide to the Adjustment Engine — architecture, workflows, "
    "database objects, and operational procedures.</span>",
    unsafe_allow_html=True,
)
st.markdown("<br/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TOC & TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_overview, tab_architecture, tab_workflow, tab_processing, tab_objects, tab_ops = st.tabs([
    "📋 Overview",
    "🏗️ Architecture",
    "🔄 Workflow & Status",
    "⚙️ Processing Engine",
    "🗄️ Database Objects",
    "🛠️ Operations Guide",
])

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 1 — OVERVIEW                                                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_overview:

    section_title("What is the Adjustment Engine?", "🎯")
    st.markdown(
        "The Adjustment Engine is a **Streamlit-on-Snowflake application** that provides a unified, "
        "self-service interface for creating, approving, reviewing, and processing risk-related "
        "adjustments across multiple scopes."
    )
    st.markdown(
        f'<div class="mcard" style="border-left: 4px solid {P["primary"]}">'
        f'<strong style="color:{P["primary"]}">Key Principle:</strong> Every adjustment starts as a row in '
        f'<code>ADJUSTMENT_APP.ADJ_HEADER</code>. The file-based interface has been removed. '
        f'Streamlit is the <em>only</em> entry point.</div>',
        unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

    # ── Scopes ────────────────────────────────────────────────────────────
    section_title("Supported Scopes", "📊")
    scope_cols = st.columns(len(SCOPE_CONFIG))
    for i, (key, cfg) in enumerate(SCOPE_CONFIG.items()):
        with scope_cols[i]:
            st.markdown(
                f'<div class="mcard" style="text-align:center;border-top:3px solid {cfg["color"]};min-height:130px">'
                f'<div style="font-size:2rem">{cfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:1rem;margin:0.3rem 0;color:{cfg["color"]}">{cfg["label"]}</div>'
                f'<div style="font-size:0.75rem;color:{P["grey_700"]}">'
                f'Config-driven via<br/><code>ADJUSTMENTS_SETTINGS</code></div>'
                f'</div>', unsafe_allow_html=True)

    _html(f"""
    <div style="background:{P['info_lt']};border:1px solid #90CAF9;border-radius:8px;padding:0.8rem 1rem;font-size:0.85rem;margin-top:0.5rem">
    <strong>Adding a new scope</strong> requires <em>only</em> a new row in
    <code>ADJUSTMENTS_SETTINGS</code> — no code changes to the Streamlit app or stored procedures.
    The processing engine reads its configuration dynamically.
    </div>
    """)

    # ── Adjustment Types ─────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Adjustment Types", "⚡")

    type_rows = ""
    for tk, tcfg in TYPE_CONFIG.items():
        type_rows += (
            f'<tr>'
            f'<td style="padding:10px 14px;font-weight:600;font-size:0.9rem;white-space:nowrap">'
            f'{tcfg["icon"]} {tk}</td>'
            f'<td style="padding:10px 14px;font-size:0.85rem;color:{P["grey_700"]}">{tcfg["desc"]}</td>'
            f'<td style="padding:10px 14px;font-family:monospace;font-size:0.82rem;'
            f'background:{P["grey_100"]};border-radius:4px">{tcfg["formula"]}</td>'
            f'</tr>')
    st.markdown(
        f'<table style="width:100%;border-collapse:collapse;border:1px solid {P["border"]};border-radius:8px;overflow:hidden">'
        f'<thead><tr style="background:{P["accent"]};color:white">'
        f'<th style="padding:10px 14px;text-align:left">Type</th>'
        f'<th style="padding:10px 14px;text-align:left">Description</th>'
        f'<th style="padding:10px 14px;text-align:left">Formula</th>'
        f'</tr></thead><tbody>{type_rows}</tbody></table>',
        unsafe_allow_html=True)

    # ── Pages Summary ────────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Application Pages", "📄")

    pages = [
        ("🏠", "Command Center",   "Dashboard with KPIs, status charts, overlap alerts, and activity feed"),
        ("✏️", "New Adjustment",    "3-step wizard: Scope & Filters → Preview → Submit"),
        ("📋", "My Work",           "All your adjustments with full history, actions, and status timeline"),
        ("✅", "Approval Queue",     "Review and approve adjustments that require approval before processing"),
        ("⏳", "Processing Queue",  "Live view of the Snowflake processing pipeline and queue position"),
        ("⚙️", "Admin",             "Settings management, recurring templates, schema reference"),
        ("📖", "Documentation",     "This page — full process guide with diagrams"),
    ]
    for icon, name, desc in pages:
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:12px;padding:0.5rem 0;'
            f'border-bottom:1px solid {P["border"]}">'
            f'<span style="font-size:1.3rem;width:32px;text-align:center">{icon}</span>'
            f'<div><div style="font-weight:600;font-size:0.9rem">{name}</div>'
            f'<div style="font-size:0.8rem;color:{P["grey_700"]}">{desc}</div></div></div>',
            unsafe_allow_html=True)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 2 — ARCHITECTURE                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_architecture:

    # ── End-to-End Architecture Diagram ──────────────────────────────────
    section_title("End-to-End Architecture", "🏗️")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:1rem 0;overflow-x:auto">
    <div style="display:flex;align-items:stretch;gap:0;min-width:900px">

        <div style="display:flex;flex-direction:column;align-items:center;min-width:140px">
            <div style="background:#E3F2FD;border:2px solid {P['info']};border-radius:10px;padding:1rem;text-align:center;width:140px">
                <div style="font-size:2rem">👤</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['info']}">User</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Streamlit UI</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Creates adjustment<br/>via 4-step wizard
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:155px">
            <div style="background:#FFF3E0;border:2px solid {P['warning']};border-radius:10px;padding:1rem;text-align:center;width:155px">
                <div style="font-size:2rem">⚡</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['warning']}">SP_SUBMIT</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Validate &amp; Insert</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Validates input<br/>Checks sign-off<br/>Inserts ADJ_HEADER
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:140px">
            <div style="background:#E8F5E9;border:2px solid {P['success']};border-radius:10px;padding:1rem;text-align:center;width:140px">
                <div style="font-size:2rem">💾</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['success']}">ADJ_HEADER</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Source of Truth</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                1 row per adjustment<br/>All dimensions + status
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:130px">
            <div style="background:#E3F2FD;border:2px solid #42A5F5;border-radius:10px;padding:1rem;text-align:center;width:130px">
                <div style="font-size:2rem">🌊</div>
                <div style="font-weight:700;font-size:0.82rem;color:#1976D2">STREAM</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">CDC Capture</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Detects INSERT<br/>and UPDATE events
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:150px">
            <div style="background:{P['purple_lt']};border:2px solid {P['purple']};border-radius:10px;padding:1rem;text-align:center;width:150px">
                <div style="font-size:2rem">⏰</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['purple']}">TASK</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Every 60 seconds</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Stream-guarded<br/>Only fires with data
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:155px">
            <div style="background:#FFEBEE;border:2px solid {P['primary']};border-radius:10px;padding:1rem;text-align:center;width:155px">
                <div style="font-size:2rem">🔄</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['primary']}">SP_PROCESS</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Python / Snowpark</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Scale / Direct logic<br/>Overlap resolution<br/>SCD2 key fix
            </div>
        </div>

        <div style="display:flex;align-items:center;padding:0 4px;font-size:1.5rem;color:{P['grey_400']}">→</div>

        <div style="display:flex;flex-direction:column;align-items:center;min-width:150px">
            <div style="background:#E8F5E9;border:2px solid {P['success']};border-radius:10px;padding:1rem;text-align:center;width:150px">
                <div style="font-size:2rem">📊</div>
                <div style="font-weight:700;font-size:0.82rem;color:{P['success']}">FACT.*_ADJ</div>
                <div style="font-size:0.7rem;color:{P['grey_700']}">Delta Rows</div>
            </div>
            <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:6px;text-align:center">
                Adjustment deltas<br/>Original data untouched
            </div>
        </div>

    </div>
    </div>
    """)

    _html(f"""
    <div class="mcard" style="border-left:4px solid {P['info']};margin-top:0.5rem">
    <strong>Delta Pattern:</strong> The original <code>FACT.*_MEASURES</code> tables are
    <em>never modified</em>. Adjustments are stored as delta rows in <code>FACT.*_ADJUSTMENT</code>.
    Downstream reporting uses <code>SUM(fact) + SUM(adjustment)</code>.
    </div>
    """)

    # ── Layer Diagram ────────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Snowflake Object Layers", "🗄️")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:0.5rem 0">

    <div style="margin-bottom:1.2rem">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['primary']};margin-bottom:0.5rem">
        ① Entry Layer — Streamlit writes here</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:#FFEBEE;border:1px solid {P['primary']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            💾 ADJ_HEADER</div>
            <div style="background:#FFEBEE;border:1px solid {P['primary']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            📝 ADJ_LINE_ITEM</div>
            <div style="background:#FFEBEE;border:1px solid {P['primary']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            📜 ADJ_STATUS_HISTORY</div>
        </div>
    </div>

    <div style="margin-bottom:1.2rem">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['purple']};margin-bottom:0.5rem">
        ② Config Layer — Drives behaviour</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:{P['purple_lt']};border:1px solid {P['purple']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⚙️ ADJUSTMENTS_SETTINGS</div>
            <div style="background:{P['purple_lt']};border:1px solid {P['purple']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            🔁 ADJ_RECURRING_TEMPLATE</div>
        </div>
    </div>

    <div style="margin-bottom:1.2rem">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['info']};margin-bottom:0.5rem">
        ③ CDC Layer — Change detection</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:#E3F2FD;border:1px solid {P['info']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            🌊 ADJ_HEADER_STREAM</div>
            <div style="background:#E3F2FD;border:1px solid {P['info']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            🌊 ADJ_LINE_ITEM_STREAM</div>
        </div>
    </div>

    <div style="margin-bottom:1.2rem">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['warning']};margin-bottom:0.5rem">
        ④ Processing Layer — Async execution</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:#FFF3E0;border:1px solid {P['warning']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⏰ PROCESS_PENDING_TASK</div>
            <div style="background:#FFF3E0;border:1px solid {P['warning']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⏰ INSTANTIATE_RECURRING_TASK</div>
            <div style="background:#FFF3E0;border:1px solid {P['warning']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⚡ SP_SUBMIT_ADJUSTMENT</div>
            <div style="background:#FFF3E0;border:1px solid {P['warning']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⚡ SP_PREVIEW_ADJUSTMENT</div>
            <div style="background:#FFF3E0;border:1px solid {P['warning']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⚡ SP_PROCESS_ADJUSTMENT</div>
        </div>
    </div>

    <div style="margin-bottom:1.2rem">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['success']};margin-bottom:0.5rem">
        ⑤ Materialised Layer — Auto-refresh (1 min lag)</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:#E8F5E9;border:1px solid {P['success']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            📊 DT_DASHBOARD</div>
            <div style="background:#E8F5E9;border:1px solid {P['success']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            ⚠️ DT_OVERLAP_ALERTS</div>
        </div>
    </div>

    <div>
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:{P['grey_700']};margin-bottom:0.5rem">
        ⑥ Read Layer — Real-time views for Streamlit</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_DASHBOARD_KPI</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_SIGNOFF_STATUS</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_RECENT_ACTIVITY</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_ERRORS</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_MY_WORK</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_PROCESSING_QUEUE</div>
            <div style="background:{P['white']};border:1px solid {P['border']};border-radius:8px;padding:0.5rem 0.8rem;font-size:0.8rem;font-weight:600">
            👁️ VW_APPROVAL_QUEUE</div>
        </div>
    </div>

    </div>
    """)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 3 — WORKFLOW & STATUS                                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_workflow:

    # ── Status State Machine ─────────────────────────────────────────────
    section_title("Adjustment Lifecycle — Status State Machine", "🔄")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:0.5rem 0;overflow-x:auto">
    <div style="display:flex;flex-direction:column;gap:1.2rem;min-width:700px">

    <div style="display:flex;align-items:center;gap:0">
        <div style="background:#FFF3E0;border:2px solid {STATUS_COLORS['Pending']};border-radius:10px;padding:0.8rem 1rem;text-align:center;min-width:120px">
            <div style="font-size:1.3rem">{STATUS_ICONS['Pending']}</div>
            <div style="font-weight:700;font-size:0.82rem;color:{STATUS_COLORS['Pending']}">Pending</div>
            <div style="font-size:0.65rem;color:{P['grey_700']}">Initial state</div>
        </div>
        <div style="text-align:center;padding:0 8px">
            <div style="font-size:1.2rem;color:{P['grey_400']}">→</div>
            <div style="font-size:0.6rem;color:{P['grey_700']}">Task fires<br/>SP_PROCESS</div>
        </div>
        <div style="background:#E8F5E9;border:2px solid {STATUS_COLORS['Processed']};border-radius:10px;padding:0.8rem 1rem;text-align:center;min-width:120px">
            <div style="font-size:1.3rem">{STATUS_ICONS['Processed']}</div>
            <div style="font-weight:700;font-size:0.82rem;color:{STATUS_COLORS['Processed']}">Processed</div>
            <div style="font-size:0.65rem;color:{P['grey_700']}">In the data ✔</div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:0;padding-left:0">
        <div style="background:#E3F2FD;border:2px solid {STATUS_COLORS['Pending Approval']};border-radius:10px;padding:0.8rem 1rem;text-align:center;min-width:120px">
            <div style="font-size:1.3rem">{STATUS_ICONS['Pending Approval']}</div>
            <div style="font-weight:700;font-size:0.78rem;color:{STATUS_COLORS['Pending Approval']}">Pending Approval</div>
            <div style="font-size:0.65rem;color:{P['grey_700']}">Optional</div>
        </div>
        <div style="text-align:center;padding:0 8px">
            <div style="font-size:1.2rem;color:{P['grey_400']}">→</div>
            <div style="font-size:0.6rem;color:{P['grey_700']}">Approver<br/>approves</div>
        </div>
        <div style="background:#E0F2F1;border:2px solid {STATUS_COLORS['Approved']};border-radius:10px;padding:0.8rem 1rem;text-align:center;min-width:120px">
            <div style="font-size:1.3rem">{STATUS_ICONS['Approved']}</div>
            <div style="font-weight:700;font-size:0.82rem;color:{STATUS_COLORS['Approved']}">Approved</div>
            <div style="font-size:0.65rem;color:{P['grey_700']}">Ready to process</div>
        </div>
        <div style="text-align:center;padding:0 8px">
            <div style="font-size:1.2rem;color:{P['grey_400']}">→</div>
            <div style="font-size:0.6rem;color:{P['grey_700']}">Task fires<br/>SP_PROCESS</div>
        </div>
        <div style="font-size:0.82rem;font-weight:700;color:{STATUS_COLORS['Processed']}">✔ Processed</div>
    </div>

    <div style="display:flex;gap:2rem;flex-wrap:wrap;padding-left:2rem">

        <div style="display:flex;align-items:center;gap:8px">
            <div style="font-size:0.72rem;color:{P['grey_700']}">Submit (COB signed off)</div>
            <div style="font-size:1rem;color:{P['grey_400']}">→</div>
            <div style="background:{P['purple_lt']};border:2px solid {STATUS_COLORS['Rejected - SignedOff']};border-radius:10px;padding:0.5rem 0.8rem;text-align:center">
                <div style="font-weight:700;font-size:0.78rem;color:{STATUS_COLORS['Rejected - SignedOff']}">🔒 Rejected - SignedOff</div>
            </div>
        </div>

        <div style="display:flex;align-items:center;gap:8px">
            <div style="font-size:0.72rem;color:{P['grey_700']}">SP_PROCESS fails</div>
            <div style="font-size:1rem;color:{P['grey_400']}">→</div>
            <div style="background:#FFEBEE;border:2px solid {STATUS_COLORS['Error']};border-radius:10px;padding:0.5rem 0.8rem;text-align:center">
                <div style="font-weight:700;font-size:0.78rem;color:{STATUS_COLORS['Error']}">❌ Error</div>
            </div>
            <div style="font-size:1rem;color:{P['grey_400']}">→</div>
            <div style="font-size:0.65rem;color:{P['grey_700']}">Retry →</div>
            <div style="font-size:0.72rem;color:{STATUS_COLORS['Pending']}">⏳ Pending</div>
        </div>

    </div>

    </div>
    </div>
    """)

    # ── Status Reference Table ───────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Status Reference", "📋")

    status_data = [
        ("Pending",              "Initial state after submission or after retry",
         "Process immediately (ad-hoc) or wait for task (recurring)"),
        ("Pending Approval",     "Submitted with requires_approval flag — awaiting approver action",
         "Approve → Approved, Reject → Rejected, Recall → Pending"),
        ("Approved",             "Approved by an authorised user — ready for processing",
         "Task picks up and processes → Processed"),
        ("Processed",            "Successfully applied to the FACT.*_ADJUSTMENT table",
         "Terminal state (adjustments are in the data)"),
        ("Error",                "SP_PROCESS_ADJUSTMENT encountered an error",
         "User can Retry → resets to Pending"),
        ("Rejected",             "Rejected by the system or an operator",
         "Terminal state"),
        ("Rejected - SignedOff", "COB was already signed off at submission time",
         "Terminal state (adjustment recorded for audit but not processed)"),
    ]

    for status, desc, transitions in status_data:
        color = STATUS_COLORS.get(status, "#9E9E9E")
        icon  = STATUS_ICONS.get(status, "•")
        st.markdown(
            f'<div style="border-left:4px solid {color};padding:0.6rem 1rem;margin-bottom:0.5rem;'
            f'background:{color}11;border-radius:0 6px 6px 0">'
            f'<span style="font-weight:700;color:{color}">{icon} {status}</span>'
            f'<br/><span style="font-size:0.82rem;color:{P["grey_700"]}">{desc}</span>'
            f'<br/><span style="font-size:0.78rem;color:{P["grey_700"]}"><strong>Transitions:</strong> {transitions}</span>'
            f'</div>', unsafe_allow_html=True)

    # ── Workflow Diagrams ────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Workflow Scenarios", "📝")

    with st.expander("**Scenario 1: Ad-hoc Scale**", expanded=True):
        _html(f"""
        <div style="background:{P['grey_100']};border-radius:10px;padding:1rem;margin:0.5rem 0">
        <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:0.82rem">
            <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-weight:600">👤 User opens wizard</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-weight:600">Selects scope + Scale type</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-weight:600">Sets filters + scale factor</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-weight:600">Previews impact (SP_PREVIEW)</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFEBEE;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['primary']}">Clicks Submit</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-weight:600">SP_SUBMIT validates + inserts</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFEBEE;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['primary']}">SP_PROCESS executes immediately</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#E8F5E9;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['success']}">✔ Processed</div>
        </div>
        </div>
        <div style="font-size:0.8rem;color:{P['grey_700']};margin-top:0.4rem">
        <strong>Key:</strong> Ad-hoc adjustments are processed immediately — SP_SUBMIT calls SP_PROCESS directly.
        The user sees the result immediately.
        </div>
        """)

    with st.expander("**Scenario 2: Recurring Adjustment**"):
        _html(f"""
        <div style="background:{P['grey_100']};border-radius:10px;padding:1rem;margin:0.5rem 0">
        <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:0.82rem">
            <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-weight:600">⚙️ Admin creates template</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:{P['purple_lt']};border-radius:6px;padding:4px 10px;font-weight:600;color:{P['purple']}">⏰ INSTANTIATE_RECURRING_TASK</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-weight:600">Inserts ADJ_HEADER (Pending)</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-weight:600">🌊 Stream detects</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:{P['purple_lt']};border-radius:6px;padding:4px 10px;font-weight:600;color:{P['purple']}">⏰ PROCESS_PENDING_TASK</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFEBEE;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['primary']}">SP_PROCESS</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#E8F5E9;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['success']}">✔ Processed</div>
        </div>
        </div>
        <div style="font-size:0.8rem;color:{P['grey_700']};margin-top:0.4rem">
        <strong>Key:</strong> Recurring adjustments are fully automatic.
        <code>INSTANTIATE_RECURRING_TASK</code> checks templates every 5 minutes and creates ADJ_HEADER
        rows when dependencies are met. <code>PROCESS_PENDING_TASK</code> processes them.
        </div>
        """)

    with st.expander("**Scenario 4: Signed-Off COB Rejection**"):
        _html(f"""
        <div style="background:{P['grey_100']};border-radius:10px;padding:1rem;margin:0.5rem 0">
        <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:0.82rem">
            <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-weight:600">👤 User submits</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-weight:600">SP_SUBMIT checks VW_SIGNOFF_STATUS</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:#FFEBEE;border-radius:6px;padding:4px 10px;font-weight:600;color:{P['danger']}">COB = SIGNED_OFF ❌</div>
            <span style="color:{P['grey_400']}">→</span>
            <div style="background:{P['purple_lt']};border-radius:6px;padding:4px 10px;font-weight:600;color:{P['purple']}">🔒 Rejected - SignedOff</div>
        </div>
        </div>
        <div style="font-size:0.8rem;color:{P['grey_700']};margin-top:0.4rem">
        <strong>Key:</strong> The adjustment is still inserted into ADJ_HEADER for audit, but
        immediately set to "Rejected - SignedOff". It is never processed.
        </div>
        """)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 4 — PROCESSING ENGINE                                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_processing:

    section_title("SP_PROCESS_ADJUSTMENT — Core Engine", "⚙️")

    _html(f"""
    The processing engine is a **Python/Snowpark stored procedure** that reads pending adjustments
    from `ADJ_HEADER` and writes delta rows to the appropriate `FACT.*_ADJUSTMENT` table.
    It supports two paths: **Direct** (Upload) and **Scale** (Scale/Flatten/Roll).
    """)

    # ── Scale Processing Flow ────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Scale Path — 3-Way UNION ALL", "📊")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:0.5rem 0;overflow-x:auto">
    <div style="display:flex;flex-direction:column;gap:1rem;min-width:600px">

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:{P['purple_lt']};border:2px solid {P['purple']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['purple']};flex-shrink:0">1</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">Read Config</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                Query <code>ADJUSTMENTS_SETTINGS</code> for this scope → get fact table name,
                PK columns, metric columns, summary table
            </div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:{P['info_lt']};border:2px solid {P['info']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['info']};flex-shrink:0">2</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">Read Pending Adjustments</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                Filter <code>ADJ_HEADER</code> where <code>RUN_STATUS = 'Pending'</code>,
                <code>COBID = target</code>, <code>PROCESS_TYPE = scope</code>
            </div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:#FFEBEE;border:2px solid {P['primary']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['primary']};flex-shrink:0">3</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">3-Way UNION ALL</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                Build a CTE with three branches joined via dimension columns:
            </div>
            <div style="display:flex;gap:8px;margin-top:6px;flex-wrap:wrap">
                <div style="background:#E3F2FD;border-radius:6px;padding:4px 10px;font-size:0.75rem;font-weight:600">
                    ① Same-COB Scale<br/>
                    <span style="font-weight:400;color:{P['grey_700']}">FACT × sf_adjusted</span>
                </div>
                <div style="background:#FFF3E0;border-radius:6px;padding:4px 10px;font-size:0.75rem;font-weight:600">
                    ② Cross-COB Scale (Roll)<br/>
                    <span style="font-weight:400;color:{P['grey_700']}">FACT_ADJ × sf_adjusted</span>
                </div>
                <div style="background:#FFEBEE;border-radius:6px;padding:4px 10px;font-size:0.75rem;font-weight:600">
                    ③ Flatten current COB<br/>
                    <span style="font-weight:400;color:{P['grey_700']}">FACT × -1 (removes existing)</span>
                </div>
            </div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:#FFF3E0;border:2px solid {P['warning']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['warning']};flex-shrink:0">4</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">Overlap Resolution (DENSE_RANK)</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                Partition by surrogate key, order by <code>ADJUSTMENT_CREATED_TIMESTAMP DESC</code>.
                Keep only <code>ROW_NUM = 1</code> → most recent adjustment wins.
            </div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:{P['purple_lt']};border:2px solid {P['purple']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['purple']};flex-shrink:0">5</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">SCD2 Key Fix (Cross-COB only)</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                For Roll adjustments: <code>TRADE_KEY</code>, <code>COMMON_INSTRUMENT_KEY</code>,
                <code>COMMON_INSTRUMENT_FCD_KEY</code> may have changed between source and target COB.
                Re-map them using dimension tables with effective dates.
            </div>
        </div>
    </div>

    <div style="display:flex;align-items:center;gap:12px">
        <div style="background:#E8F5E9;border:2px solid {P['success']};border-radius:50%;width:36px;height:36px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:0.85rem;color:{P['success']};flex-shrink:0">6</div>
        <div style="flex:1">
            <div style="font-weight:700;font-size:0.88rem">Insert, Update Status, Rebuild Summary</div>
            <div style="font-size:0.78rem;color:{P['grey_700']}">
                Delete old adjustments for this COBID+ADJ_ID → insert new rows from temp table →
                rebuild summary → update <code>ADJ_HEADER.RUN_STATUS = 'Processed'</code> →
                log to <code>ADJ_STATUS_HISTORY</code>
            </div>
        </div>
    </div>

    </div>
    </div>
    """)

    # ── Direct Processing Flow ───────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Direct Path — Upload / Line Items", "📤")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:0.5rem 0">
    <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:0.82rem">
        <div style="background:#E3F2FD;border-radius:6px;padding:6px 12px;font-weight:600">User uploads CSV</div>
        <span style="color:{P['grey_400']}">→</span>
        <div style="background:#FFF3E0;border-radius:6px;padding:6px 12px;font-weight:600">Streamlit parses + UNPIVOTs</div>
        <span style="color:{P['grey_400']}">→</span>
        <div style="background:#E3F2FD;border-radius:6px;padding:6px 12px;font-weight:600">Writes to ADJ_LINE_ITEM</div>
        <span style="color:{P['grey_400']}">→</span>
        <div style="background:#FFF3E0;border-radius:6px;padding:6px 12px;font-weight:600">SP_PROCESS reads line items</div>
        <span style="color:{P['grey_400']}">→</span>
        <div style="background:#FFEBEE;border-radius:6px;padding:6px 12px;font-weight:600">check_columns() maps to fact schema</div>
        <span style="color:{P['grey_400']}">→</span>
        <div style="background:#E8F5E9;border-radius:6px;padding:6px 12px;font-weight:600;color:{P['success']}">Inserts to FACT.*_ADJ</div>
    </div>
    </div>

    <div class="mcard" style="border-left:4px solid {P['info']};margin-top:0.5rem">
    <strong>Direct vs Scale:</strong><br/>
    • <strong>Scale:</strong> SP reads the original fact table, multiplies by <code>scale_factor_adjusted</code>, and writes the delta.<br/>
    • <strong>Direct:</strong> SP reads <code>ADJ_LINE_ITEM</code> (user-provided values) and maps them to the fact schema columns.
    </div>
    """)

    # ── Scale Factor Computation ─────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Scale Factor Computation", "🧮")

    _html(f"""
    <table style="width:100%;border-collapse:collapse;border:1px solid {P['border']};border-radius:8px;overflow:hidden;font-size:0.85rem">
    <thead><tr style="background:{P['accent']};color:white">
        <th style="padding:10px 14px;text-align:left">Type</th>
        <th style="padding:10px 14px;text-align:left">User Input (sf)</th>
        <th style="padding:10px 14px;text-align:left">scale_factor_adjusted</th>
        <th style="padding:10px 14px;text-align:left">Effect</th>
    </tr></thead>
    <tbody>
    <tr style="border-bottom:1px solid {P['border']}">
        <td style="padding:10px 14px;font-weight:600">Flatten</td>
        <td style="padding:10px 14px">Any (ignored)</td>
        <td style="padding:10px 14px;font-family:monospace;font-weight:700;color:{P['primary']}">-1</td>
        <td style="padding:10px 14px;color:{P['grey_700']}">fact × (-1) = negate everything → net zero</td>
    </tr>
    <tr style="border-bottom:1px solid {P['border']}">
        <td style="padding:10px 14px;font-weight:600">Scale (same COB)</td>
        <td style="padding:10px 14px">1.05</td>
        <td style="padding:10px 14px;font-family:monospace;font-weight:700;color:{P['primary']}">sf - 1 = 0.05</td>
        <td style="padding:10px 14px;color:{P['grey_700']}">fact × 0.05 = adds +5% as delta</td>
    </tr>
    <tr style="border-bottom:1px solid {P['border']}">
        <td style="padding:10px 14px;font-weight:600">Roll (cross COB)</td>
        <td style="padding:10px 14px">1.0</td>
        <td style="padding:10px 14px;font-family:monospace;font-weight:700;color:{P['primary']}">sf = 1.0</td>
        <td style="padding:10px 14px;color:{P['grey_700']}">Copy 100% from source COB → target COB (+ flatten existing)</td>
    </tr>
    <tr>
        <td style="padding:10px 14px;font-weight:600">Upload / Direct</td>
        <td style="padding:10px 14px">N/A</td>
        <td style="padding:10px 14px;font-family:monospace;font-weight:700;color:{P['primary']}">0 (not used)</td>
        <td style="padding:10px 14px;color:{P['grey_700']}">User-provided values written directly</td>
    </tr>
    </tbody></table>
    """)

    _html(f"""
    <div class="mcard" style="border-left:4px solid {P['warning']};margin-top:0.5rem">
    <strong>Why sf - 1 for same-COB?</strong><br/>
    Because the original fact data already exists. To scale by 1.05×, we only need
    to add the <em>incremental</em> 0.05× as a delta:
    <code>original + (original × 0.05) = original × 1.05</code>.
    <br/><br/>
    For cross-COB (Roll), the original data on the target COB may be different,
    so we flatten it first (branch ③) and then add the full source amount (branch ②).
    </div>
    """)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 5 — DATABASE OBJECTS                                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_objects:

    section_title("Complete Object Inventory", "🗄️")

    _html(f"""
    All objects live in database <code>DVLP_RAPTOR_NEWADJ</code>, schema <code>ADJUSTMENT</code>
    (except target FACT.* tables which are in existing schemas).
    """)

    objects = [
        # (Name, Type, Script, Description)
        ("ADJ_HEADER",              "TABLE",           "01_tables.sql",   "Single entry point for all adjustments. One row per adjustment with all dimension filters, status, and metadata. Autoincrement starts at 200000."),
        ("ADJ_LINE_ITEM",           "TABLE",           "01_tables.sql",   "Detail rows for Direct/Upload adjustments. Each row = one dimension combination with its adjustment value."),
        ("ADJ_STATUS_HISTORY",      "TABLE",           "01_tables.sql",   "Complete audit trail of every status transition. Append-only."),
        ("ADJUSTMENTS_SETTINGS",    "TABLE",           "01_tables.sql",   "Config: maps each scope to its fact/adjustment tables, metrics, and PKs. Adding a new scope = new row."),
        ("ADJ_RECURRING_TEMPLATE",  "TABLE",           "01_tables.sql",   "Templates for recurring adjustments. Instantiated by INSTANTIATE_RECURRING_TASK."),
        ("ADJ_HEADER_STREAM",       "STREAM",          "02_streams.sql",  "INSERT+UPDATE CDC on ADJ_HEADER. Guards PROCESS_PENDING_TASK (only fires when there's data)."),
        ("ADJ_LINE_ITEM_STREAM",    "STREAM",          "02_streams.sql",  "APPEND_ONLY stream on ADJ_LINE_ITEM."),
        ("SP_SUBMIT_ADJUSTMENT",    "PROCEDURE",       "03_sp_submit.sql","Entry point from Streamlit. Validates JSON, checks sign-off, inserts ADJ_HEADER, triggers SP_PROCESS for ad-hoc."),
        ("SP_PREVIEW_ADJUSTMENT",   "PROCEDURE",       "04_sp_preview.sql","Read-only preview. Shows CURRENT, ADJUSTMENT, and PROJECTED values. RETURNS TABLE."),
        ("SP_PROCESS_ADJUSTMENT",   "PROCEDURE",       "05_sp_process.sql","Core engine. Scale path: 3-way UNION ALL + DENSE_RANK overlap + SCD2 fix. Direct path: maps line items to fact schema."),
        ("PROCESS_PENDING_TASK",    "TASK",            "06_tasks.sql",    "Runs every 60s (stream-guarded). Processes recurring adjustments via SP_PROCESS."),
        ("INSTANTIATE_RECURRING_TASK","TASK",           "06_tasks.sql",    "Runs every 5 min. Creates ADJ_HEADER rows from ADJ_RECURRING_TEMPLATE."),
        ("DT_DASHBOARD",            "DYNAMIC TABLE",   "07_dynamic.sql",  "Pre-aggregated metrics by scope/status/entity/user. 1-min refresh."),
        ("DT_OVERLAP_ALERTS",       "DYNAMIC TABLE",   "07_dynamic.sql",  "Self-join overlap detection with wildcard matching. 1-min refresh."),
        ("ADJ_SIGNOFF_STATUS",      "TABLE",           "01_tables.sql",   "COB sign-off status per scope. Managed via Admin page."),
        ("VW_SIGNOFF_STATUS",       "VIEW",            "08_views.sql",    "Reads ADJ_SIGNOFF_STATUS for sign-off checks."),
        ("VW_DASHBOARD_KPI",        "VIEW",            "08_views.sql",    "Aggregated KPIs per COB: counts by status, total value, avg processing time, overlap alerts."),
        ("VW_RECENT_ACTIVITY",      "VIEW",            "08_views.sql",    "UNION of submissions + status transitions for the dashboard timeline."),
        ("VW_ERRORS",               "VIEW",            "08_views.sql",    "Adjustments currently in Error status."),
        ("VW_MY_WORK",              "VIEW",            "08_views.sql",    "All adjustments — Streamlit filters by CURRENT_USER()."),
        ("VW_PROCESSING_QUEUE",     "VIEW",            "08_views.sql",    "Live queue with QUEUE_POSITION via ROW_NUMBER."),
        ("VW_APPROVAL_QUEUE",       "VIEW",            "08_views.sql",    "Adjustments awaiting approval. Filtered by RUN_STATUS = Pending Approval."),
    ]

    type_colors = {
        "TABLE": P["primary"], "STREAM": "#42A5F5", "PROCEDURE": P["warning"],
        "TASK": P["purple"], "DYNAMIC TABLE": P["success"], "VIEW": P["grey_700"],
    }

    for name, obj_type, script, desc in objects:
        tc = type_colors.get(obj_type, P["grey_700"])
        st.markdown(
            f'<div style="border-left:4px solid {tc};padding:0.5rem 1rem;margin-bottom:0.4rem;'
            f'background:{tc}08;border-radius:0 6px 6px 0">'
            f'<div style="display:flex;justify-content:space-between;align-items:center">'
            f'<span style="font-weight:700;font-size:0.88rem;color:{tc}">{name}</span>'
            f'<span style="font-size:0.7rem;font-weight:600;background:{tc}22;color:{tc};'
            f'padding:2px 8px;border-radius:10px">{obj_type}</span>'
            f'</div>'
            f'<div style="font-size:0.78rem;color:{P["grey_700"]};margin-top:2px">{desc}</div>'
            f'<div style="font-size:0.68rem;color:{P["grey_400"]};margin-top:2px">Source: {script}</div>'
            f'</div>', unsafe_allow_html=True)

    # ── Key relationships ────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Object Relationships", "🔗")

    _html(f"""
    <div style="background:{P['grey_100']};border-radius:12px;padding:1.5rem;margin:0.5rem 0;overflow-x:auto">
    <div style="font-family:monospace;font-size:0.78rem;line-height:2;color:{P['grey_900']};min-width:600px">

    <div style="font-weight:700;color:{P['primary']}">ADJ_HEADER (PK: ADJ_ID)</div>
    <div style="padding-left:20px">├── <span style="color:{P['info']}">ADJ_LINE_ITEM</span>.ADJ_ID → ADJ_HEADER.ADJ_ID <span style="color:{P['grey_700']}"> (Upload detail rows)</span></div>
    <div style="padding-left:20px">├── <span style="color:{P['info']}">ADJ_STATUS_HISTORY</span>.ADJ_ID → ADJ_HEADER.ADJ_ID <span style="color:{P['grey_700']}"> (audit trail)</span></div>
    <div style="padding-left:20px">├── <span style="color:#42A5F5">ADJ_HEADER_STREAM</span> → watches ADJ_HEADER <span style="color:{P['grey_700']}"> (CDC events)</span></div>
    <div style="padding-left:20px">├── <span style="color:{P['success']}">DT_DASHBOARD</span> ← reads ADJ_HEADER <span style="color:{P['grey_700']}"> (1-min materialisation)</span></div>
    <div style="padding-left:20px">├── <span style="color:{P['success']}">DT_OVERLAP_ALERTS</span> ← self-join on ADJ_HEADER <span style="color:{P['grey_700']}"> (overlap detection)</span></div>
    <div style="padding-left:20px">├── <span style="color:{P['grey_700']}">VW_*</span> ← various views reading ADJ_HEADER <span style="color:{P['grey_700']}"> (real-time queries)</span></div>
    <div style="padding-left:20px">└── <span style="color:{P['warning']}">SP_PROCESS_ADJUSTMENT</span> → writes to FACT.*_ADJUSTMENT <span style="color:{P['grey_700']}"> (delta rows)</span></div>

    <div style="margin-top:1rem;font-weight:700;color:{P['purple']}">ADJUSTMENTS_SETTINGS (PK: PROCESS_TYPE)</div>
    <div style="padding-left:20px">├── <span style="color:{P['warning']}">SP_SUBMIT_ADJUSTMENT</span> → validates scope is active</div>
    <div style="padding-left:20px">└── <span style="color:{P['warning']}">SP_PROCESS_ADJUSTMENT</span> → reads fact table name, PKs, metrics</div>

    </div>
    </div>
    """)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 6 — OPERATIONS GUIDE                                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_ops:

    section_title("Common Operations", "🛠️")

    with st.expander("**How to: Retry a failed adjustment**", expanded=True):
        st.markdown("""
        1. Go to **📋 My Work** → **❌ Errors / Rejected** tab
        2. Find the failed adjustment and expand it
        3. Review the error message in the red box
        4. Fix the underlying issue (e.g., missing data, dimension misconfig)
        5. Click **🔄 Retry** — this resets the status to `Pending`
        6. The processing task will pick it up within 60 seconds
        """)
        st.markdown(
            f'<div class="mcard" style="border-left:4px solid {P["info"]};margin-top:0.5rem;font-size:0.85rem">'
            f'<strong>SQL equivalent:</strong><br/>'
            f'<code>UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_STATUS = \'Pending\', ERRORMESSAGE = NULL WHERE ADJ_ID = &lt;id&gt;;</code>'
            f'</div>',
            unsafe_allow_html=True)

    with st.expander("**How to: Add a new scope**"):
        st.markdown(f"""
        1. Ensure the fact table exists (e.g., `FACT.NEWSCOPE_MEASURES`)
        2. Create the adjustment table (e.g., `FACT.NEWSCOPE_MEASURES_ADJUSTMENT`)
        3. Insert a new row into `ADJUSTMENTS_SETTINGS`:

        ```sql
        INSERT INTO ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
            (PROCESS_TYPE, FACT_TABLE, FACT_ADJUSTED_TABLE, FACT_TABLE_PK,
             ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE, ADJUSTMENT_BASE_TABLE,
             METRIC_NAME, METRIC_USD_NAME, IS_ACTIVE)
        VALUES
            ('NewScope',
             'FACT.NEWSCOPE_MEASURES',
             'FACT.NEWSCOPE_MEASURES_ADJUSTMENT',
             'ENTITY_KEY;BOOK_KEY;TRADE_KEY',
             'FACT.NEWSCOPE_MEASURES_ADJUSTMENT',
             NULL,
             'ADJUSTMENT_APP.ADJ_HEADER',
             'MEASURE_VALUE',
             'MEASURE_VALUE_IN_USD',
             TRUE);
        ```

        4. The Streamlit app will automatically show the new scope — **no code changes needed**.
        """)

    with st.expander("**How to: Create a recurring template**"):
        st.markdown(f"""
        1. Go to **⚙️ Admin** → **Recurring Templates** tab
        2. Fill in the template form with the scope, type, and filter dimensions
        3. The `INSTANTIATE_RECURRING_TASK` will automatically create `ADJ_HEADER` rows
           each day (every 5 minutes it checks for templates)
        4. Once created, the `PROCESS_PENDING_TASK` will process them

        **Or via SQL:**
        ```sql
        INSERT INTO ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE
            (TEMPLATE_NAME, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
             SCALE_FACTOR, ENTITY_CODE, REASON, IS_ACTIVE, CREATED_BY)
        VALUES
            ('Daily VaR MUSE Flatten', 'VaR', 'Flatten', 'Scale',
             1, 'MUSE', 'Daily flatten for MUSE entity', TRUE, CURRENT_USER());
        ```
        """)

    with st.expander("**How to: Enable/disable tasks**"):
        st.markdown("""
        Tasks are deployed in a **SUSPENDED** state. To enable:

        ```sql
        ALTER TASK ADJUSTMENT_APP.PROCESS_PENDING_TASK       RESUME;
        ALTER TASK ADJUSTMENT_APP.INSTANTIATE_RECURRING_TASK RESUME;
        ```

        To suspend (e.g., for maintenance):
        ```sql
        ALTER TASK ADJUSTMENT_APP.PROCESS_PENDING_TASK       SUSPEND;
        ALTER TASK ADJUSTMENT_APP.INSTANTIATE_RECURRING_TASK SUSPEND;
        ```

        **Check task history:**
        ```sql
        SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            TASK_NAME => 'PROCESS_PENDING_TASK',
            SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
        )) ORDER BY SCHEDULED_TIME DESC;
        ```
        """)

    with st.expander("**How to: Investigate an overlap**"):
        st.markdown(f"""
        1. Go to **🏠 Command Center** → Overlap Alerts panel
        2. Note the two ADJ_IDs that overlap
        3. In Snowflake, query the details:

        ```sql
        SELECT * FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS
        WHERE ADJ_ID_A = <id1> OR ADJ_ID_B = <id1>;
        ```

        4. The **most recent** adjustment (by `ADJUSTMENT_CREATED_TIMESTAMP`) always wins
           due to `DENSE_RANK` in the processing engine
        5. If the older adjustment should win, delete the newer one from **📋 My Work**
        """)

    with st.expander("**How to: Understand the audit trail**"):
        st.markdown(f"""
        Every status change is logged in `ADJ_STATUS_HISTORY`:

        ```sql
        SELECT h.*, a.PROCESS_TYPE, a.ADJUSTMENT_TYPE, a.ENTITY_CODE
        FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY h
        JOIN ADJUSTMENT_APP.ADJ_HEADER a ON a.ADJ_ID = h.ADJ_ID
        WHERE h.ADJ_ID = <id>
        ORDER BY h.CHANGED_AT;
        ```

        You can also see the full timeline in the **📋 My Work** page by expanding
        any adjustment card → **Status History** section.
        """)

    # ── Design Principles ────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Design Principles", "💡")

    principles = [
        ("Delta Pattern",       "Original FACT tables are never modified. Adjustments store only the offset. Reporting uses SUM(fact) + SUM(adjustment)."),
        ("Config-Driven",       "ADJUSTMENTS_SETTINGS maps scopes to tables. Adding a new scope = one INSERT, zero code changes."),
        ("Streamlit-First",     "ADJ_HEADER is the single entry point. No file shares, no staging tables, no CSV drops."),
        ("Async Processing",    "Snowflake Tasks process adjustments asynchronously. Users are never blocked waiting for large operations."),
        ("Full Audit Trail",    "Every status transition is logged in ADJ_STATUS_HISTORY. Adjustments are soft-deleted, never hard-deleted."),
        ("Overlap Resolution",  "When two adjustments target the same data, DENSE_RANK ensures the most recent one wins automatically."),
        ("Sign-Off Guard",      "SP_SUBMIT checks ADJ_SIGNOFF_STATUS. If COB is signed off, the adjustment is recorded but rejected. Managed via Admin page."),
        ("Optional Approval",   "When requires_approval is set, adjustments go through Pending Approval → Approved before processing. Approval is optional — unchecked adjustments skip straight to Pending → Processed."),
    ]

    for title, desc in principles:
        st.markdown(
            f'<div style="display:flex;gap:12px;align-items:flex-start;padding:0.5rem 0;'
            f'border-bottom:1px solid {P["border"]}">'
            f'<div style="background:{P["primary"]};color:white;border-radius:50%;width:8px;height:8px;'
            f'margin-top:7px;flex-shrink:0"></div>'
            f'<div><span style="font-weight:700;font-size:0.88rem">{title}</span>'
            f'<br/><span style="font-size:0.82rem;color:{P["grey_700"]}">{desc}</span></div>'
            f'</div>', unsafe_allow_html=True)
