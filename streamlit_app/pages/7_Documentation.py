"""
Documentation — Full Process Guide
=====================================
Rewritten to describe the system AS BUILT:
  • polling pipeline (1-minute scope tasks, claim tokens — no streams)
  • sign-off lifecycle (external feed → block → re-open approval → re-sign-off)
  • "All FRTB" fan-out (one adjustment per real sub-type; FRTBALL retired)
  • delete/retry semantics, the stale-run reaper, force-process
  • PowerBI refresh hand-off and statuses

Organised for two readers: analysts (Overview / Creating / Approvals &
Sign-off / Troubleshooting) and maintainers (Processing Engine / Reference).
"""
import streamlit as st


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

from utils.styles import (
    inject_css, render_sidebar, section_title,
    P, SCOPE_CONFIG, STATUS_COLORS, icon,
)

inject_css()
render_sidebar()

st.markdown("## Documentation")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "How the Adjustment Engine works — creating adjustments, approvals and "
    "sign-off, the processing pipeline, reports, and what to do when something "
    "goes wrong.</span>",
    unsafe_allow_html=True,
)
st.markdown("<br/>", unsafe_allow_html=True)


# ── Small HTML building blocks ────────────────────────────────────────────────

def _card(body: str, accent: str = None) -> str:
    border = f"border-left:4px solid {accent};" if accent else ""
    return (f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
            f'{border}border-radius:8px;padding:0.9rem 1.1rem;margin-bottom:0.8rem;'
            f'font-size:0.85rem;line-height:1.55">{body}</div>')


def _table(headers, rows) -> str:
    th = "".join(
        f'<th style="text-align:left;padding:6px 10px;font-size:0.74rem;'
        f'text-transform:uppercase;letter-spacing:.05em;color:{P["grey_700"]};'
        f'border-bottom:2px solid {P["border"]}">{h}</th>' for h in headers)
    trs = ""
    for r in rows:
        tds = "".join(
            f'<td style="padding:6px 10px;font-size:0.82rem;'
            f'border-bottom:1px solid {P["border"]};vertical-align:top">{c}</td>'
            for c in r)
        trs += f"<tr>{tds}</tr>"
    return (f'<div style="overflow-x:auto"><table style="width:100%;'
            f'border-collapse:collapse;background:{P["white"]}">'
            f'<thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>')


def _flow(steps) -> str:
    """Horizontal step flow: [(title, subtitle), ...]."""
    cells = ""
    for i, (title, sub) in enumerate(steps):
        arrow = ('<div style="align-self:center;color:' + P["grey_400"]
                 + ';font-size:1.1rem;padding:0 2px">→</div>') if i else ""
        cells += (arrow +
                  f'<div style="flex:1;background:{P["white"]};border:1px solid {P["border"]};'
                  f'border-top:3px solid {P["primary"]};border-radius:8px;padding:0.6rem;'
                  f'text-align:center;min-width:110px">'
                  f'<div style="font-weight:700;font-size:0.8rem">{title}</div>'
                  f'<div style="font-size:0.7rem;color:{P["grey_700"]};margin-top:2px">{sub}</div>'
                  f'</div>')
    return (f'<div style="display:flex;gap:6px;align-items:stretch;'
            f'margin:0.6rem 0 1rem 0;flex-wrap:wrap">{cells}</div>')


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 10px;font-size:0.74rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

(tab_overview, tab_create, tab_approval, tab_processing,
 tab_reports, tab_trouble, tab_reference) = st.tabs([
    "Overview",
    "Creating Adjustments",
    "Approvals & Sign-Off",
    "Processing Engine",
    "Reports (PowerBI)",
    "Troubleshooting",
    "Reference",
])

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 1 — OVERVIEW                                                          ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_overview:
    section_title("What is the Adjustment Engine?", "target")
    _html(_card(
        "The Adjustment Engine lets the risk team correct or override published "
        "risk numbers — VaR, Stress, Sensitivity and FRTB — in a controlled, "
        "fully audited way. Instead of editing fact data directly, you submit an "
        "<strong>adjustment ticket</strong>; the engine validates it, optionally "
        "routes it for approval, applies it to the adjustment tables (never the "
        "original fact data), and queues a PowerBI report refresh. Every step is "
        "recorded: who submitted, who approved, when it processed, and when the "
        "reports picked it up."))

    _html(_flow([
        ("Submit", "New Adjustment page"),
        ("Approve", "if required / Entity Roll"),
        ("Process", "task polls every 1 min"),
        ("Reports", "PowerBI refresh queued"),
    ]))

    section_title("Supported Scopes", "bar-chart")
    scope_rows = []
    for name, cfg in SCOPE_CONFIG.items():
        scope_rows.append([
            f'{icon(cfg.get("icon", ""), size=13, color=cfg.get("color", "#333"))} '
            f'<strong>{name}</strong>',
            cfg.get("desc", "—"),
        ])
    _html(_table(["Scope", "Description"], scope_rows))
    _html(_card(
        f'{icon("info", size=13, color=P["info"])} The three FRTB sub-types '
        f'(FRTB, FRTBDRC, FRTBRRAO) are processed by one shared FRTB pipeline. '
        f'Choosing <strong>“All FRTB”</strong> on the New Adjustment page simply '
        f'creates one adjustment per sub-type — three sibling tickets that '
        f'process one after another.', P["info"]))

    section_title("Adjustment Categories", "zap")
    cat_rows = [
        ["<strong>Scaling Adjustment</strong>",
         "Scale, flatten or roll fact-table data by a factor. "
         "<em>Flatten</em> zeroes the selected scope; <em>Scale</em> multiplies "
         "it; <em>Roll</em> carries another COB's adjusted values forward.",
         "No (optional)"],
        ["<strong>Direct Adjustment</strong>",
         "Paste or upload a CSV of exact values for Stress, Sensitivity or "
         "FRTB. Each row is validated and submitted as its own, independent "
         "adjustment.",
         "No (optional)"],
        ["<strong>VaR Upload</strong>",
         "Upload one CSV in VaR's legacy layout — the whole file becomes a "
         "single adjustment entry. A re-upload with the same COB + Reference "
         "<em>replaces</em> the previous upload.",
         "No (optional)"],
        ["<strong>Entity Roll</strong>",
         "Replace an entity's entire figures at a COB with another COB's "
         "adjusted figures. Destructive: it removes every existing adjustment "
         "for that entity/COB first.",
         "Always"],
    ]
    _html(_table(["Category", "What it does", "Approval required"], cat_rows))

    section_title("Application Pages", "file-text")
    _html(_table(["Page", "What you do there"], [
        ["<strong>Home</strong>", "Dashboard: KPIs, recent activity, overlap alerts."],
        ["<strong>New Adjustment</strong>",
         "Create and submit adjustments; request a COB re-open; sign a "
         "re-opened COB off again."],
        ["<strong>Adjustments</strong>",
         "Browse everything, view history, and act: Retry failed, Delete, "
         "Submit for approval, Recall."],
        ["<strong>Approval Queue</strong>",
         "Approvers approve/reject adjustments and COB re-open requests "
         "(4-eyes: never your own)."],
        ["<strong>Adjustment Pipeline</strong>",
         "Live processing view: what needs attention, what is in flight, "
         "per-adjustment deep dive, force-process."],
        ["<strong>Admin</strong>",
         "Restricted: scope config, sign-off override, approvers, page "
         "administrators."],
        ["<strong>Logs</strong>", "Processing runs, activity feed, and errors."],
    ]))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 2 — CREATING ADJUSTMENTS                                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_create:
    section_title("The New Adjustment Page", "info")
    _html(_card(
        "One dense screen: pick the <strong>Category</strong> (Scaling / Direct "
        "/ VaR Upload / Entity Roll), pick the <strong>Scope</strong>, fill the fields, and "
        "the live <strong>Ticket</strong> panel on the right tracks completeness "
        "and unlocks Submit when everything required is present. Every "
        "adjustment needs an <strong>Adjustment Category</strong> (the managed "
        "business-reason list) and a <strong>Reason</strong>."))

    section_title("Scaling Adjustment", "scale")
    _html(_table(["Type", "Effect", "Factor applied by the engine"], [
        ["<strong>Flatten</strong>", "Zeroes the selected scope at the COB "
         "(combined value becomes 0).", "−1"],
        ["<strong>Scale</strong>", "Multiplies the selected scope. Factor 1.10 "
         "means +10%.", "factor − 1 (incremental delta)"],
        ["<strong>Roll</strong>", "Carries the source COB's <em>adjusted</em> "
         "values (original + its adjustments) forward to the target COB, "
         "flattening what was there.", "full factor on the source leg"],
    ]))
    _html(_card(
        f'{icon("eye", size=13, color=P["info"])} <strong>Impact preview:</strong> '
        f'for narrow scopes (book/department level) run the preview before '
        f'submitting — it shows rows affected and the value delta. A preview '
        f'matching <strong>0 rows</strong> blocks Submit: one of your filter '
        f'codes probably does not exist at that COB.', P["info"]))
    _html(_card(
        f'{icon("layers", size=13, color="#7E22CE")} <strong>All FRTB:</strong> '
        f'selecting “All FRTB” submits three sibling adjustments — one each for '
        f'FRTB, FRTBDRC and FRTBRRAO. They overlap by design, so the pipeline '
        f'runs them in sequence; the preview shown is the sum across the three.',
        "#7E22CE"))

    section_title("Direct Adjustment (paste or upload CSV)", "list")
    _html(_card(
        "Paste or upload a CSV of exact values for Stress, Sensitivity or "
        "FRTB (the expected columns are shown on the page). Each row is "
        "validated independently and becomes its own adjustment — a 200-row "
        "file submits 200 separate tickets, each with its own USD value, "
        "not one combined adjustment.<br/><br/>"
        "There is no reference-based replacement here: to correct a row, "
        "submit a new one or act on the existing ticket from the "
        "<strong>Adjustments</strong> page (Retry, Delete, Recall)."))

    section_title("VaR Upload (CSV file)", "upload")
    _html(_card(
        "Upload one CSV in VaR's legacy layout (the expected columns are "
        "shown on the page). The whole file is stored exactly as uploaded "
        "and written to the VaR adjustment table when processed as a "
        "<strong>single</strong> adjustment entry.<br/><br/>"
        "<strong>Reference &amp; replacement:</strong> the Reference field "
        "identifies the upload. Submitting a new VaR Upload with the same "
        "<strong>COB + Reference</strong> replaces the previous one — the "
        "old ticket is marked <em>Replaced</em> and its rows are removed from "
        "the adjustment tables in the same transaction. The page warns you and "
        "asks for explicit confirmation before a replacement. A replacement is "
        "refused while the previous upload is mid-processing."))

    section_title("Entity Roll", "refresh-cw")
    _html(_card(
        f'{icon("alert-triangle", size=13, color=P["danger"])} '
        f'<strong>Destructive and always approval-gated.</strong> An Entity '
        f'Roll makes the entity\'s adjusted view at the target COB mirror the '
        f'source COB. To do that it first <em>permanently removes every '
        f'existing adjustment</em> for that entity at the target COB — '
        f'including data loaded by other systems. The page shows a '
        f'reconciliation count of what will be removed and requires a ticked '
        f'confirmation; the whole wipe + roll is applied atomically (all or '
        f'nothing). Large entities (e.g. MUSI on VaR) move hundreds of '
        f'millions of rows and can take ~20 minutes to process.', P["danger"]))

    section_title("What happens at Submit", "send")
    _html(_table(["Check", "Outcome"], [
        ["Scope active", "Inactive/unknown scope → submission refused."],
        ["Sign-off", "COB signed off (in-app or by the upstream publish feed) "
         "→ ticket stored as <em>Rejected - SignedOff</em>; request a re-open "
         "instead (see Approvals &amp; Sign-Off)."],
        ["Approval", "Entity Roll, or the “Requires Approval” tick → status "
         "<em>Pending Approval</em> (Approval Queue). Otherwise → "
         "<em>Pending</em>, picked up by the next 1-minute poll."],
        ["Overlap", "If a Pending/Running adjustment touches the same data "
         "(same COB, overlapping filters), the new one is queued behind it and "
         "the ticket says which adjustment it is waiting for. It starts "
         "automatically when the blocker finishes."],
    ]))
    _html(_card(
        f'{icon("clock", size=13, color=P["grey_700"])} <strong>Recurring '
        f'adjustments are not live yet</strong> — the option stores the range '
        f'but automatic daily instantiation is planned for a future release. '
        f'Submit each COB\'s adjustment manually for now.'))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 3 — APPROVALS & SIGN-OFF                                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_approval:
    section_title("Adjustment Approval (4-eyes)", "check-circle")
    _html(_card(
        "Adjustments submitted with <em>Requires Approval</em> (and every "
        "Entity Roll) wait in the <strong>Approval Queue</strong>. Only users "
        "registered as approvers — optionally per scope — can decide, and "
        "<strong>never their own submissions</strong>. Approving moves the "
        "adjustment to <em>Approved</em> and the pipeline processes it within "
        "a minute; rejecting records the reason. If two approvers race, only "
        "the first decision counts — the second sees that the request already "
        "moved on, and no misleading audit entry is written."))

    section_title("COB Sign-Off Lifecycle", "lock")
    _html(_card(
        "Sign-off is owned by the upstream publish system shared across many "
        "consumers — the first sign-off always comes from there (a unified "
        "file feed the engine syncs every 30 minutes, and also checks live at "
        "submit time). Sign-off granularity is <strong>COB + entity + "
        "scope</strong> (one FRTB entry covers FRTBDRC and FRTBRRAO). Once "
        "signed off, <strong>no new adjustments can be submitted</strong> for "
        "that entity — an adjustment with no entity filter is blocked by any "
        "signed-off entity it would touch."))
    _html(_flow([
        ("Signed off", "upstream feed"),
        ("Re-open requested", "New Adjustment page"),
        ("Approved", "Approval Queue, 4-eyes"),
        ("Re-opened", "adjustments allowed"),
        ("Signed off again", "from the app"),
    ]))
    _html(_table(["Status", "Meaning", "Can submit?"], [
        [_pill("OPEN", P["success"]),
         "COB open per the upstream feed (synced in) — not yet signed off.", "Yes"],
        [_pill("SIGNOFF_REQUESTED", "#B45309"),
         "An app sign-off request is awaiting approval.", "No"],
        [_pill("SIGNED_OFF", P["danger"]),
         "Signed off (upstream feed, or app request approved).", "No"],
        [_pill("REOPEN_REQUESTED", "#B45309"),
         "A re-open request is awaiting approval.", "No"],
        [_pill("REOPENED", P["info"]),
         "Re-open approved — adjust, then request sign-off again.", "Yes"],
    ]))
    _html(_card(
        "<strong>Both lifecycle changes are approval-gated requests</strong> "
        "(the checkboxes on the Sign-Off page are ticked and locked by "
        "policy): <em>request sign-off</em> on an OPEN/REOPENED entry, or "
        "<em>request re-open</em> on a SIGNED_OFF one — from the Sign-Off "
        "page (re-open also from the New Adjustment panel). An approver who "
        "is not the requester actions it in the Approval Queue's <em>COB "
        "Sign-Off / Re-open Requests</em> section. Rejection returns the "
        "entry to its previous status. Every transition is recorded in the "
        "sign-off history."))

    section_title("Who can do what", "user")
    _html(_table(["Action", "Who"], [
        ["Submit adjustments, request COB re-open, re-sign-off a re-opened COB",
         "Any app user"],
        ["Approve/reject adjustments and re-open requests",
         "Registered approvers (per scope), never for their own requests"],
        ["View COB sign-off status", "Any app user (Sign-Off page)"],
        ["Manage approvers, scope config, page admins",
         "Page administrators (Admin page; access controlled by the "
         "administrators list, with a warning-flagged open ‘bootstrap’ mode "
         "until the first admin is registered)"],
        ["Sign-off overrides, upstream sync, manual sign-off entries",
         "Page administrators (Sign-Off page, admin-gated controls)"],
    ]))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 4 — PROCESSING ENGINE                                                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_processing:
    section_title("Polling Pipeline (no streams)", "settings")
    _html(_card(
        "Four scope tasks (VaR, Stress, FRTB, Sensitivity) run every minute on "
        "serverless compute and call <code>SP_RUN_PIPELINE</code>, which "
        "<strong>polls</strong> the queue — it reads eligible rows straight "
        "from <code>ADJ_HEADER</code>, so nothing can be stranded by event "
        "plumbing. An idle poll exits in milliseconds. VaR and Sensitivity run "
        "on LARGE compute (Entity Rolls move ~900M rows per leg); the tasks "
        "have an explicit 3-hour timeout so a long roll finishes rather than "
        "being killed."))
    _html(_flow([
        ("Reap", "auto-fail dead runs"),
        ("Serialise", "block overlapping rows"),
        ("Claim", "token-stamp eligible rows"),
        ("Process", "one call per scope/action/COB"),
        ("Release", "unblock waiting rows"),
    ]))
    _html(_table(["Step", "What it does"], [
        ["<strong>Reap</strong>",
         "Any row stuck in <em>Running</em> for over 4 hours is reset to "
         "<em>Failed</em> with a clear message (the run that claimed it died — "
         "e.g. a kill or crash). This releases everything queued behind it; "
         "the user just clicks Retry."],
        ["<strong>Serialise</strong>",
         "Adjustments touching the same data (same COB + overlapping filters) "
         "must not run at once. The oldest proceeds; newer ones are marked "
         "blocked-by and start automatically when the blocker finishes. Direct "
         "and VaR Upload rows don't overlap-serialise: Direct rows are "
         "independent per-row adjustments (no dedupe needed); VaR Upload's "
         "duplicate control is the Reference."],
        ["<strong>Claim</strong>",
         "Eligible Pending/Approved rows are atomically promoted to "
         "<em>Running</em> and stamped with the run's unique claim token. Each "
         "processing call only ever touches rows carrying its own token — two "
         "concurrent runs can never double-apply or fail each other's work."],
        ["<strong>Process</strong>",
         "<code>SP_PROCESS_ADJUSTMENT</code> runs once per (scope, action, "
         "COB) combo — combos run in parallel. It registers the dimension "
         "row, applies the Scale / Direct / Upload / Entity Roll logic, "
         "updates statuses and history, closes the run log, and queues the "
         "PowerBI refresh."],
        ["<strong>Release</strong>",
         "Rows whose blocker just finished have their block cleared; the next "
         "1-minute poll picks them up."],
    ]))

    section_title("How each action is applied", "zap")
    _html(_table(["Action", "Mechanics (plain language)"], [
        ["<strong>Scale / Flatten</strong>",
         "The engine reads the matching fact rows, multiplies by the "
         "effective factor (−1 for flatten, factor−1 for same-COB scale), nets "
         "the result to one delta row per position, and writes it to the "
         "scope's adjustment table. Where a new adjustment covers the same "
         "positions as an older one, the newest wins — the older adjustment's "
         "overlapping rows are superseded, never double-counted."],
        ["<strong>Roll (cross-COB)</strong>",
         "Two legs: flatten the target COB's current values, then carry the "
         "source COB's <em>adjusted</em> values forward (× factor). Combined "
         "result at the target = the source COB's adjusted numbers."],
        ["<strong>Direct</strong>",
         "One header row = one fact row: the codes on the adjustment resolve "
         "directly to dimension keys (case-insensitive, −1 when blank or "
         "unmatched) and its USD value lands straight in the measure column "
         "— no intermediate line items, no FX conversion."],
        ["<strong>Upload</strong>",
         "The uploaded file's line items are transformed via the scope's "
         "configured mapping (dimension codes resolved to keys) and inserted "
         "with the adjustment's report ID as one entry. A retry after a "
         "failure first removes anything the failed run wrote — retries "
         "never double-count."],
        ["<strong>Entity Roll</strong>",
         "Atomic destructive replace: remove every adjustment for the entity "
         "at the target COB, then insert a flatten of the entity's base values "
         "plus a copy of the source COB's adjusted values. Processes "
         "<strong>one roll per run</strong> — a second approved roll re-queues "
         "for the next minute. All-or-nothing: a failure rolls the whole "
         "operation back."],
    ]))

    section_title("Deleting an adjustment", "trash")
    _html(_card(
        "Delete (Adjustments page, confirmation required) soft-deletes the "
        "ticket <em>and removes the adjustment's rows from the adjustment and "
        "summary tables</em> in one transaction — reports stop including it "
        "after the next refresh. If any part fails, nothing is removed and "
        "the page says so. The audit history always survives."))

    section_title("Timing & compute", "clock")
    _html(_table(["Setting", "Value", "Why"], [
        ["Scope task schedule", "1 minute", "Fast pickup with millisecond idle polls."],
        ["VaR / Sensitivity compute", "LARGE (serverless)",
         "Entity Rolls move ~900M rows per leg — MEDIUM overran the timeout."],
        ["Task timeout", "3 hours", "A long roll must finish, not be killed."],
        ["Stale-run reaper", "4 hours",
         "Above the task ceiling so a live run is never reaped; dead runs "
         "auto-reset to Failed."],
        ["Sign-off feed sync", "30 minutes",
         "Materialises upstream sign-offs; the submit gate also checks the "
         "feed live, so the block holds between syncs."],
    ]))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 5 — REPORTS (POWERBI)                                                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_reports:
    section_title("From Processed to Reports", "line-chart")
    _html(_card(
        "After an adjustment finishes processing, the engine hands the change "
        "to reporting by one of two paths, depending on the scope:"))
    _html(_table(["Scopes", "Hand-off", "How it works"], [
        ["<strong>VaR, Stress</strong>", "Power BI refresh",
         "A row is written to the PowerBI action queue "
         "(<code>METADATA.POWERBI_ACTION</code>) whose empty start time marks "
         "it as <em>pending</em>. The refresh scheduler picks pending actions "
         "up (~every 5 minutes), stamps start and completion times, and the "
         "refreshed reports include the adjustment."],
        ["<strong>Sensitivity, FRTB / DRC / RRAO</strong>", "dbt rebuild via Control-M",
         "A dummy dataset row (<code>DUMMY_Sensitivity_Adjustment</code> / "
         "<code>DUMMY_FRTB_Adjustment</code>) is written to "
         "<code>RAVEN.LOG_STAGE_ME_STATUS</code>. A Control-M job polls that "
         "table, finds the new record and starts the dbt job that rebuilds "
         "the reporting model. All three FRTB scopes share the FRTB trigger."],
    ]))
    _html(_flow([
        ("Processed", "engine run complete"),
        ("Queued / Triggered", "PBI action or dbt trigger written"),
        ("Refreshing / Rebuilding", "PBI refresh or dbt job runs"),
        ("Reports Ready", "reports include the adjustment"),
    ]))
    _html(_table(["Report status", "Meaning"], [
        [_pill("Awaiting", P["grey_700"]),
         "Processed, hand-off not yet queued/matched. During BST the status "
         "display can lag up to an hour (timezone alignment); the refresh "
         "itself is unaffected."],
        [_pill("Queued", P["warning"]), "PBI refresh action created, waiting for the scheduler."],
        [_pill("Refreshing", P["info"]), "PowerBI dataset refresh in progress."],
        [_pill("Reports Ready", P["success"]), "PBI refresh complete — reports include the adjustment."],
        [_pill("Rebuild Triggered", P["success"]),
         "dbt trigger written — Control-M detects it and runs the dbt "
         "rebuild. Progress beyond this point is tracked in Control-M/dbt, "
         "not in the app."],
    ]))
    _html(_card(
        f'{icon("alert-triangle", size=13, color="#B45309")} If the hand-off '
        f'fails (queueing the PBI refresh, or writing the dbt trigger), the '
        f'adjustment stays <em>Processed</em> (its numbers ARE applied) but '
        f'the failure is stamped on the ticket\'s error field and shown on '
        f'the Adjustments / Pipeline pages — reports may show stale data '
        f'until the next scheduled refresh. PBI refresh actions are only '
        f'queued for recent COBs (~3 business days); older back-dated '
        f'adjustments rely on the next scheduled full refresh.', "#B45309"))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 6 — TROUBLESHOOTING                                                   ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_trouble:
    section_title("Something looks wrong — what do I do?", "help-circle")
    _html(_table(["Symptom", "Why it happens", "What to do"], [
        ["Adjustment sits in <strong>Pending</strong> for minutes",
         "Usually it is <em>blocked</em> behind another adjustment touching "
         "the same data, or awaiting approval.",
         "Pipeline page → the item shows who it is waiting for. Blocked rows "
         "start automatically. If it is genuinely stranded, use "
         "<em>Force process</em>."],
        ["Adjustment shows <strong>Failed</strong>",
         "The processing run hit an error (message shown on the ticket).",
         "Adjustments page → select it → <strong>Retry</strong>. Retries are "
         "safe: anything a failed run wrote is cleaned up first. If it keeps "
         "failing, the error text says why — contact support with the ADJ id."],
        ["Adjustment stuck in <strong>Running</strong> for hours",
         "The run that claimed it died (timeout, kill). Big Entity Rolls "
         "legitimately run ~20 min — hours means dead.",
         "Nothing required: the pipeline auto-resets it to Failed after 4 "
         "hours and frees anything queued behind it; then Retry. Past the "
         "4-hour mark you can also Force process it immediately."],
        ["Submission returns <strong>Rejected - SignedOff</strong>",
         "The COB is signed off for that scope (upstream publish sign-off or "
         "in-app).",
         "New Adjustment page → the sign-off panel → <em>Request re-open</em> "
         "with a reason; an approver actions it in the Approval Queue. After "
         "adjusting, sign the COB off again from the same panel."],
        ["My approval/reject seemed to do nothing",
         "Someone else decided it first, or the submitter recalled it.",
         "The page tells you the request already moved on — no false audit "
         "entry is written. Refresh and check its current state."],
        ["VaR Upload replaced the wrong thing / warning about replacement",
         "A VaR Upload with the same COB + Reference already existed.",
         "The replacement needs an explicit confirmation tick. Use a new "
         "Reference if it is genuinely a different adjustment."],
        ["Reports don't show my processed adjustment",
         "Refresh still queued/running, or refresh queueing failed (error "
         "shown on the ticket), or the COB is older than the ~3-business-day "
         "refresh window.",
         "Pipeline page → deep dive → PowerBI Refresh section shows queued / "
         "started / completed times."],
    ]))

    section_title("Escalation data to include", "clipboard")
    _html(_card(
        "When raising an issue, include: the <strong>ADJ report ID</strong> "
        "(e.g. #1234, shown across the app), the COB, the scope, the status, "
        "and the error text from the ticket. The Logs page has the matching "
        "run-log entries; Entity Roll runs additionally log every step live "
        "with durations and row counts."))

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 7 — REFERENCE                                                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝

with tab_reference:
    section_title("Adjustment Status Reference", "clipboard")
    status_desc = {
        "Pending":              "Queued — the next 1-minute poll picks it up.",
        "Pending Approval":     "Waiting in the Approval Queue.",
        "Approved":             "Approved — queued for the next poll.",
        "Running":              "Processing right now (claimed by a pipeline run).",
        "Processed":            "Applied to the adjustment tables; report refresh queued.",
        "Failed":               "Processing errored — see the ticket's error text; Retry re-queues.",
        "Rejected":             "An approver rejected it (reason recorded).",
        "Rejected - SignedOff": "Refused at submission because the COB is signed off.",
        "Replaced":             "Superseded by a newer VaR Upload with the same COB + Reference.",
        "Superseded":           "Removed by an Entity Roll that rebuilt the entity at that COB.",
        "Deleted":              "Deleted by a user; its rows were removed from the adjustment tables.",
    }
    srows = []
    for name, desc in status_desc.items():
        color = STATUS_COLORS.get(name, P["grey_700"])
        srows.append([_pill(name, color), desc])
    _html(_table(["Status", "Meaning"], srows))

    section_title("Key Database Objects", "database")
    _html(_table(["Object", "Role"], [
        ["<code>ADJ_HEADER</code>",
         "Single entry point for every adjustment — one row per ticket, with "
         "status, claim token, and blocking info."],
        ["<code>ADJ_LINE_ITEM_JSON</code>", "VaR Upload rows (verbatim CSV payload per row)."],
        ["<code>ADJ_STATUS_HISTORY</code>", "Append-only audit of every status transition."],
        ["<code>ADJUSTMENTS_SETTINGS</code>",
         "Per-scope config: fact/adjustment/summary tables, metric columns, "
         "surrogate key definition, active flag."],
        ["<code>ADJ_SIGNOFF_STATUS</code> / <code>ADJ_SIGNOFF_HISTORY</code>",
         "Sign-off lifecycle per COB + entity + scope ('*' = whole scope) + its audit trail."],
        ["<code>ADJ_APPROVERS</code> / <code>ADJ_ADMINS</code> / <code>ADJ_CATEGORY</code>",
         "Approver registry (per scope), Admin-page access list, managed "
         "business-category list."],
        ["<code>SP_SUBMIT_ADJUSTMENT</code>",
         "Validates, checks sign-off (app + upstream feed), handles VaR "
         "Upload replacement transactionally, inserts the ticket."],
        ["<code>SP_RUN_PIPELINE</code> + 4 scope tasks",
         "1-minute polling orchestrator: reap → serialise → claim (token) → "
         "process → release."],
        ["<code>SP_PROCESS_ADJUSTMENT</code>",
         "Core engine: Scale / Direct / Upload / Entity Roll application, "
         "netting and supersede, run log, PowerBI hand-off."],
        ["<code>SP_FORCE_PROCESS_ADJUSTMENT</code>",
         "Manual escape hatch — pushes one stuck adjustment through now "
         "(refuses to bypass a live blocker)."],
        ["<code>SP_SYNC_SIGNOFF_STATUS</code> + <code>TASK_SYNC_SIGNOFF</code>",
         "30-minute sync of upstream publish sign-offs into the app."],
        ["<code>SP_PREVIEW_ADJUSTMENT</code>", "Read-only impact preview for the New Adjustment page."],
        ["<code>VW_ADJUSTMENT_TRACK</code> and friends",
         "Reporting views: full lifecycle per adjustment (incl. PowerBI "
         "refresh status and blocking), queues, KPIs, errors."],
        ["<code>DT_DASHBOARD</code> / <code>DT_OVERLAP_ALERTS</code>",
         "Dynamic tables (1-minute lag) backing the Home dashboard."],
        ["<code>EROL_PROCESS_LOG</code>",
         "Live per-step diagnostics for Entity Roll runs (duration, rows, "
         "query id — visible while a step is still running)."],
    ]))

    section_title("Design Principles", "info")
    _html(_table(["Principle", "In practice"], [
        ["Fail loud, never silently wrong",
         "Errors surface on the ticket and in history; partial writes roll "
         "back; a stale click never fakes success or writes false audit rows."],
        ["Base data is never modified",
         "All adjustments live in separate adjustment tables; reports combine "
         "them with the untouched fact data."],
        ["Newest wins, exactly once",
         "Overlapping adjustments are serialised, netted per position, and "
         "superseded — never double-counted; retries clean up first."],
        ["Everything audited",
         "Status history, sign-off history, approver identity, run logs, and "
         "per-step Entity Roll diagnostics."],
        ["Config-driven scopes",
         "Adding/altering a scope is settings data (tables, metrics, keys), "
         "not code."],
    ]))
