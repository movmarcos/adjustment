"""
Documentation — Full Process Guide
=====================================
Rewritten to describe the system AS BUILT:
  • polling pipeline (1-minute scope tasks, claim tokens — no streams)
  • sign-off lifecycle (external feed → block → re-open approval → re-sign-off)
  • multi-scope submission (one adjustment per selected scope; FRTBALL retired)
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

from utils.styles import (scope_label, scope_meta, wide_kwargs, 
    inject_css, render_sidebar, section_title,
    P, SCOPE_CONFIG, icon, bordered_container, status_badge,
)
from utils.snowflake_conn import run_query, run_query_df, friendly_error

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
# AI ASSISTANT — Snowflake Cortex, grounded in a live snapshot of the engine.
# Runs entirely inside Snowflake (SNOWFLAKE.CORTEX.COMPLETE): the data never
# leaves the account. Answers are constrained to the injected context so it
# reports real status instead of guessing.
# ══════════════════════════════════════════════════════════════════════════════

def _cfg(key, default=""):
    try:
        r = run_query(f"""SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
                          WHERE CONFIG_KEY = '{key}'""")
        return str(r[0][0]) if r and r[0][0] is not None else default
    except Exception:
        return default


def _sql_lit(s: str) -> str:
    """Escape a Python string for a Snowflake single-quoted literal."""
    return str(s).replace("\\", "\\\\").replace("'", "''")


# What the system IS — a compact, always-true description so the model can
# explain processes even when the question is not about live data.
_KNOWLEDGE = """
The MUFG Adjustment Engine lets the risk team correct/override published risk
numbers for four scopes — VaR, Stress, Sensitivity, FRTB (FRTB has sub-types
FRTB=SBM, FRTBDRC, FRTBRRAO, processed by one shared FRTB pipeline). Base fact
data is NEVER modified; adjustments live in separate adjustment tables and
reports combine them.

Adjustment categories:
- Scaling: Flatten (zero the scope), Scale (multiply by a factor), Roll (carry
  another COB's adjusted values forward).
  Scaling and Entity Roll accept SEVERAL data scopes at once — the app
  creates one adjustment per selected scope. With several scopes the filter
  form offers only the fields every selected scope supports.
- Direct: exact values. For VaR/Stress/Sensitivity it is PER ROW — paste or
  upload a CSV and each row becomes its own independent adjustment. For
  FRTB/FRTBDRC/FRTBRRAO it is PER FILE — one uploaded file = one Direct
  adjustment (type "Direct"); the columns are the scope's upload template, the
  COB is read-only and comes from the file's COBID column, and a new file with
  the same COB + Reference replaces the previous submission.
- VaR Upload: one CSV in VaR legacy layout = one adjustment; re-upload with the
  same COB+Reference replaces the previous one.
- Entity Roll: destructive replace of an entity's figures at a COB; always
  needs approval.

Lifecycle statuses: Pending (queued for the 1-min poll), Pending Approval,
Approved, Running, Processed (applied; report refresh queued), Failed (see the
error; Retry re-queues and cleans up first), Rejected, Rejected - SignedOff
(COB was signed off), Replaced, Superseded, Deleted.

Processing: four scope tasks run every minute (serverless) and call
SP_RUN_PIPELINE which polls ADJ_HEADER — reap dead runs (stuck >4h -> Failed),
serialise overlapping rows, claim eligible rows with a token, process, release
blocked rows. Overlapping adjustments are serialised and the newest wins:
when it is processed, every earlier adjustment row at that COB inside its
filter is removed (superseded by filter scope, never double-counted).

Sign-off: owned by the upstream publish feed (synced every 30 min; also checked
live at submit). Granularity is COB+entity+scope (+optional SUB_TYPE). Once
signed off, no new adjustments can be submitted for that entity. Sign-off can
apply immediately (approval optional); re-open ALWAYS needs 4-eyes approval.
Who may sign off or request a re-open is the "Authorized Sign-Off Users" list
on the Admin page (optionally per scope); while that list is empty everyone
may. Approving those requests is a separate list: "Authorized Approvers".

Reports hand-off: VaR/Stress -> Power BI refresh (~5 min); Sensitivity/FRTB ->
dbt rebuild trigger via Control-M. If hand-off fails the numbers are still
applied but reports may be stale.

Home / System Status: the System Status indicator follows the selected COB
range. A Failed adjustment can be ACKNOWLEDGED on Home (Current Errors ->
"Acknowledge a failure") with a note: System Status returns to HEALTHY while
the failure stays listed (tagged ACK). If a Retry fails again the failure is
re-armed and counts again until acknowledged again. Once a failure is handled
(retried or accepted), acknowledge it on Home so System Status returns to
healthy.

Pages: Home (dashboard, System Status, acknowledge failures), New Adjustment
(create/submit), Adjustments (pipeline status boxes + stage board + full list
with Retry/Delete/Recall), Approval Queue (approve/reject, 4-eyes), Sign-Off
(sign off — approval optional; request re-open — approval required; sync),
FRTB Explore (browse the OFFICIAL FRTB fact tables by COB / entity / risk
class / sensitivity type / book / trade, with summary KPIs, a grid and a CSV
download of max 1,000 rows whose columns are exactly the FRTB upload template —
edit the file and re-upload it as a Direct adjustment), Admin (config,
approvers, admins), Logs (runs, activity, errors, sign-off audit), Tasks & Cost
(task health + serverless cost). Approvers never approve their own requests.
""".strip()


def _live_snapshot(question: str, user: str = "") -> str:
    """Compact, current facts pulled from the engine so answers are grounded in
    real state. Each query is best-effort — a missing grant never breaks it."""
    import re
    parts = []

    def _try(label, sql, fmt):
        try:
            df = run_query_df(sql)
            if df is not None and not df.empty:
                parts.append(label + "\n" + fmt(df))
        except Exception:
            pass

    _try("Adjustment counts by status (not deleted):",
         """SELECT RUN_STATUS, COUNT(*) AS N FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE COALESCE(IS_DELETED, FALSE) = FALSE GROUP BY RUN_STATUS
            ORDER BY N DESC""",
         lambda d: "; ".join(f"{r.RUN_STATUS}: {int(r.N)}"
                             for r in d.itertuples()))

    _try("Most recent adjustments (id, COB, scope, type, status, entity, by):",
         """SELECT DIMENSION_ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                   RUN_STATUS, ENTITY_CODE, USERNAME
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE COALESCE(IS_DELETED, FALSE) = FALSE
            ORDER BY CREATED_DATE DESC LIMIT 15""",
         lambda d: "\n".join(
             f"  #{r.DIMENSION_ADJ_ID} COB {r.COBID} {r.PROCESS_TYPE} "
             f"{r.ADJUSTMENT_TYPE} {r.RUN_STATUS} "
             f"entity={r.ENTITY_CODE or '-'} by {r.USERNAME}"
             for r in d.itertuples()))

    _try("Recent failures (id, scope, error):",
         """SELECT DIMENSION_ADJ_ID, PROCESS_TYPE,
                   LEFT(COALESCE(ERRORMESSAGE,''),140) AS ERR
            FROM ADJUSTMENT_APP.VW_ERRORS ORDER BY ERROR_TIME DESC LIMIT 8""",
         lambda d: "\n".join(f"  #{r.DIMENSION_ADJ_ID} {r.PROCESS_TYPE}: {r.ERR}"
                             for r in d.itertuples()))

    _try("Sign-off status (recent COBs):",
         """SELECT COBID, PROCESS_TYPE, COALESCE(ENTITY_CODE,'*') AS ENTITY_CODE,
                   SIGN_OFF_STATUS
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
            ORDER BY COBID DESC, PROCESS_TYPE LIMIT 25""",
         lambda d: "\n".join(
             f"  COB {r.COBID} {r.PROCESS_TYPE} {r.ENTITY_CODE}: {r.SIGN_OFF_STATUS}"
             for r in d.itertuples()))

    # If the question names a specific adjustment id (#123) or a COB (8 digits),
    # pull that row's full lifecycle so the answer can be specific.
    _ids = re.findall(r"#?\b(\d{1,7})\b", question or "")
    _cobs = [i for i in _ids if len(i) == 8]
    _adj = [i for i in _ids if len(i) < 8]
    if _adj:
        _in = ",".join(str(int(x)) for x in _adj[:5])
        _try(f"Details for adjustment id(s) {_in}:",
             f"""SELECT DIMENSION_ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                        CURRENT_STAGE, RUN_STATUS, ENTITY_CODE, SUBMITTED_BY,
                        APPROVED_BY, PROCESSING_ENDED_AT, REPORT_STATUS,
                        LEFT(COALESCE(ERRORMESSAGE,''),160) AS ERR
                 FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
                 WHERE DIMENSION_ADJ_ID IN ({_in})""",
             lambda d: "\n".join(
                 f"  #{r.DIMENSION_ADJ_ID} COB {r.COBID} {r.PROCESS_TYPE} "
                 f"{r.ADJUSTMENT_TYPE} stage={r.CURRENT_STAGE} status={r.RUN_STATUS} "
                 f"entity={r.ENTITY_CODE or '-'} by {r.SUBMITTED_BY} "
                 f"approved_by={r.APPROVED_BY or '-'} report={r.REPORT_STATUS or '-'} "
                 f"err={r.ERR or '-'}"
                 for r in d.itertuples()))
    if _cobs:
        _in = ",".join(str(int(x)) for x in _cobs[:3])
        _try(f"Adjustments on COB(s) {_in}:",
             f"""SELECT DIMENSION_ADJ_ID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                        RUN_STATUS, ENTITY_CODE
                 FROM ADJUSTMENT_APP.ADJ_HEADER
                 WHERE COBID IN ({_in}) AND COALESCE(IS_DELETED,FALSE)=FALSE
                 ORDER BY CREATED_DATE DESC LIMIT 30""",
             lambda d: "\n".join(
                 f"  #{r.DIMENSION_ADJ_ID} {r.PROCESS_TYPE} {r.ADJUSTMENT_TYPE} "
                 f"{r.RUN_STATUS} entity={r.ENTITY_CODE or '-'}"
                 for r in d.itertuples()))

    _try("Waiting for approval (id, COB, scope, type, submitted by):",
         """SELECT DIMENSION_ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, USERNAME
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE RUN_STATUS = 'Pending Approval'
              AND COALESCE(IS_DELETED, FALSE) = FALSE
            ORDER BY CREATED_DATE DESC LIMIT 10""",
         lambda d: "\n".join(
             f"  #{r.DIMENSION_ADJ_ID} COB {r.COBID} {r.PROCESS_TYPE} "
             f"{r.ADJUSTMENT_TYPE} by {r.USERNAME}"
             for r in d.itertuples()))

    # "my adjustments" — the asker's own recent submissions.
    if user and re.search(r"\b(my|mine|i submitted|i created)\b",
                          question or "", re.I):
        _try(f"Recent adjustments submitted by the asker ({user}):",
             f"""SELECT DIMENSION_ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                        RUN_STATUS, ENTITY_CODE
                 FROM ADJUSTMENT_APP.ADJ_HEADER
                 WHERE UPPER(USERNAME) = '{_sql_lit(user.upper())}'
                   AND COALESCE(IS_DELETED, FALSE) = FALSE
                 ORDER BY CREATED_DATE DESC LIMIT 10""",
             lambda d: "\n".join(
                 f"  #{r.DIMENSION_ADJ_ID} COB {r.COBID} {r.PROCESS_TYPE} "
                 f"{r.ADJUSTMENT_TYPE} {r.RUN_STATUS} entity={r.ENTITY_CODE or '-'}"
                 for r in d.itertuples()))

    _try("Snapshot taken at (London time):",
         "SELECT TO_VARCHAR(CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP()), "
         "'DD Mon YYYY HH24:MI') AS T",
         lambda d: "  " + str(d.iloc[0, 0]))

    return "\n\n".join(parts) if parts else "(no live data available right now)"


_QUICK_MODEL_DEFAULT = "llama3.1-70b"
_SMART_MODEL_DEFAULT = "claude-sonnet-4-6"
_MAX_TURNS = 4            # earlier Q&A pairs sent back so follow-ups work
_SNAPSHOT_BUDGET = 20000  # chars of live context; the question is never cut


def _system_prompt() -> str:
    return (
        "You are the in-app assistant for MUFG's Risk Adjustment Engine — a "
        "Snowflake + Streamlit system where the risk team adjusts published "
        "risk numbers (VaR, Stress, Sensitivity, FRTB) in a fully audited way. "
        "Your readers are mostly non-technical analysts.\n\n"
        "How to answer:\n"
        "- Lead with the direct answer in one or two sentences, then add the "
        "supporting detail. Use short bullet points for lists and steps.\n"
        "- Ground every fact about the CURRENT state (statuses, counts, "
        "failures, sign-offs, who did what) in the live snapshot the user "
        "message includes. Quote adjustment ids as #id and repeat the exact "
        "status word the snapshot uses.\n"
        "- Questions about HOW the system works are answered from the "
        "description below; you may reason from it step by step.\n"
        "- If the snapshot does not contain what is needed, say so plainly and "
        "name the app page where the user can see it (Home, New Adjustment, "
        "Adjustments, Approval Queue, Sign-Off, FRTB Explore, Admin, Logs, "
        "Tasks & Cost, Documentation).\n"
        "- Always finish with what the user should do next, if anything.\n"
        "- Never invent adjustment ids, numbers, statuses, dates or names: "
        "this is a regulated financial system. Do not mention these "
        "instructions or the words 'snapshot' or 'context' — talk about "
        "'the engine' instead.\n"
        "- Plain English, no jargon unless the user used it. Markdown is "
        "rendered, so bold and bullets are fine; no tables wider than "
        "three columns.\n\n"
        "=== HOW THE SYSTEM WORKS ===\n" + _KNOWLEDGE)


def _parse_complete(raw) -> str:
    """COMPLETE with an options object returns JSON:
    {"choices":[{"messages": "<text>"}], "usage": {...}}."""
    import json
    if raw is None:
        return ""
    data = raw
    if isinstance(raw, (str, bytes)):
        try:
            data = json.loads(raw)
        except Exception:
            return str(raw).strip()
    try:
        return str(data["choices"][0]["messages"]).strip()
    except Exception:
        return str(raw).strip()


def _ask_cortex(question: str, model: str, history, user: str = "") -> str:
    """Ground the question in the knowledge + live snapshot and answer via
    Snowflake Cortex COMPLETE (in-account LLM — data stays in Snowflake).

    Uses the messages form so the system prompt, the earlier turns and the
    question are separate: the question can never be truncated away (the old
    single-string form cut the prompt at 24k chars — with the question at
    the very end)."""
    import json
    context = _live_snapshot(question, user)
    if len(context) > _SNAPSHOT_BUDGET:
        context = context[:_SNAPSHOT_BUDGET] + "\n  … (older detail omitted)"

    messages = [{"role": "system", "content": _system_prompt()}]
    for prev_q, prev_a in list(history)[-_MAX_TURNS:]:
        messages.append({"role": "user", "content": prev_q})
        messages.append({"role": "assistant", "content": prev_a})
    messages.append({"role": "user", "content":
        "=== CURRENT STATE OF THE ENGINE (live, read just now) ===\n"
        + context + "\n\n=== QUESTION ===\n" + (question or "")})
    options = {"temperature": 0.2, "max_tokens": 1500}

    sql = (f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{_sql_lit(model)}', "
           f"PARSE_JSON('{_sql_lit(json.dumps(messages))}')::ARRAY, "
           f"PARSE_JSON('{_sql_lit(json.dumps(options))}')::OBJECT) AS ANSWER")
    rows = run_query(sql)
    answer = _parse_complete(rows[0][0]) if rows else ""
    return answer or "The assistant returned no answer."


def _is_model_error(ex: Exception) -> bool:
    """Cortex raises when a model name is unknown or not served in this
    region (cross-region inference off). Distinguish that from 'Cortex is
    not enabled at all' so the UI can fall back to the quick model."""
    m = str(ex).lower()
    return "model" in m and any(w in m for w in (
        "unknown", "not supported", "unsupported", "not available",
        "invalid", "not found", "region"))


def _render_ai_assistant() -> None:
    if _cfg("AI_ASSISTANT_ENABLED", "true").strip().lower() != "true":
        return
    quick_model = (_cfg("AI_ASSISTANT_MODEL", _QUICK_MODEL_DEFAULT).strip()
                   or _QUICK_MODEL_DEFAULT)
    smart_model = (_cfg("AI_ASSISTANT_SMART_MODEL", _SMART_MODEL_DEFAULT).strip()
                   or _SMART_MODEL_DEFAULT)
    try:
        from utils.snowflake_conn import current_user_name
        user = current_user_name() or ""
    except Exception:
        user = ""

    section_title("Ask the Assistant (AI)", "zap")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.85rem'>"
        "Ask about an adjustment's status, why something is blocked, how a "
        "process works, or what to do next. Powered by Snowflake Cortex — it "
        "runs inside Snowflake, so your data never leaves the account, and it "
        "answers from the engine's live state. Follow-up questions remember "
        "the earlier ones in this conversation.</span>",
        unsafe_allow_html=True)

    # Session state. "ai_q" is OWNED by the text_input widget below, so it may
    # only be written from a widget callback (on_click) — callbacks run before
    # any widget is drawn. Writing it from the button's return branch, after
    # the text_input exists, raises StreamlitAPIException ("cannot be
    # modified after the widget with key ai_q is instantiated").
    st.session_state.setdefault("ai_q", "")
    st.session_state.setdefault("ai_history", [])   # [(q, a, model), ...]
    st.session_state.setdefault("ai_submit", False)

    def _pick_example(q: str) -> None:
        st.session_state["ai_q"] = q
        st.session_state["ai_submit"] = True

    def _clear_conversation() -> None:
        st.session_state["ai_q"] = ""
        st.session_state["ai_history"] = []
        st.session_state["ai_submit"] = False

    with bordered_container():
        q = st.text_input(
            "Your question", key="ai_q",
            placeholder="e.g. What is the status of adjustment #1234? "
                        "Why is my VaR adjustment blocked? Which COBs are signed off?")
        c1, c2, c3 = st.columns([1, 2, 3])
        with c1:
            _go = st.button("Ask", type="primary", **wide_kwargs(),
                            disabled=not q.strip())
        with c2:
            smart = st.checkbox(
                "Think harder", key="ai_smart",
                help="Sends the question to a larger model — better for "
                     "multi-step or 'why' questions. Slower (10–30 s).")
        with c3:
            st.caption(f"Model: {smart_model if smart else quick_model}")

        # Quick-start example chips — clicking one asks it straight away.
        st.markdown(
            f"<div style='font-size:0.72rem;color:{P['grey_700']};margin:2px 0'>"
            "Try:</div>", unsafe_allow_html=True)
        _examples = [
            "How many adjustments failed and why?",
            "Which COBs are currently signed off?",
            "How does the sign-off re-open process work?",
            "What should I do if an adjustment is stuck in Running?",
        ]
        ec = st.columns(len(_examples))
        for _c, _q in zip(ec, _examples):
            _c.button(_q, key=f"ai_ex_{hash(_q) & 0xffff}", **wide_kwargs(),
                      on_click=_pick_example, args=(_q,))

        submitted = (_go or st.session_state.get("ai_submit")) and q.strip()
        st.session_state["ai_submit"] = False

        if submitted:
            question = q.strip()
            model = smart_model if smart else quick_model
            prior = [(h[0], h[1]) for h in st.session_state["ai_history"]]
            note = ""
            with st.spinner("Thinking… (reading the engine and asking Cortex)"):
                try:
                    try:
                        answer = _ask_cortex(question, model, prior, user)
                    except Exception as ex:
                        if smart and _is_model_error(ex):
                            # Larger model not served here — answer anyway.
                            model = quick_model
                            answer = _ask_cortex(question, model, prior, user)
                            note = (f"The larger model ({smart_model}) is not "
                                    f"available in this Snowflake region yet, so "
                                    f"this was answered with {quick_model}. An "
                                    f"admin can pick another model under Admin › "
                                    f"Notifications › AI Assistant.")
                        else:
                            raise
                    st.session_state["ai_history"].append(
                        (question, answer, model))
                except Exception as ex:
                    if _is_model_error(ex):
                        st.warning(
                            f"The model '{model}' is not available in this "
                            f"Snowflake account/region. An admin can change it "
                            f"under Admin › Notifications › AI Assistant "
                            f"(config keys AI_ASSISTANT_MODEL / "
                            f"AI_ASSISTANT_SMART_MODEL). ({friendly_error(ex)})")
                    else:
                        st.warning(
                            "The assistant is unavailable. This usually means "
                            "Snowflake Cortex is not enabled for this "
                            "account/region or the app role lacks the "
                            "SNOWFLAKE.CORTEX_USER role. An admin can turn it "
                            f"off in Admin config. ({friendly_error(ex)})")
            if note:
                st.caption(note)

        # Conversation so far — newest last, answers rendered as markdown.
        hist = st.session_state["ai_history"]
        if hist:
            for i, (hq, ha, hm) in enumerate(hist):
                is_last = i == len(hist) - 1
                st.markdown(
                    f'<div style="margin-top:0.7rem;font-size:0.8rem;'
                    f'color:{P["grey_700"]}"><strong>You asked:</strong> '
                    f'{__import__("html").escape(hq)}</div>',
                    unsafe_allow_html=True)
                with bordered_container():
                    st.markdown(ha)
                    st.caption(
                        f"AI-generated from the engine's live state by {hm} — "
                        "verify anything critical against the source page."
                        if is_last else f"Answered by {hm}")
            st.button("Clear conversation", key="ai_clear",
                      on_click=_clear_conversation)


_render_ai_assistant()
st.markdown("<br/>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

(tab_overview, tab_create, tab_approval, tab_processing,
 tab_reports, tab_trouble, tab_reference) = st.tabs([
    "Overview",
    "Creating Adjustments",
    "Approvals & Sign-Off",
    "Processing Engine",
    "Reports (Power BI / dbt)",
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
        "original fact data), and hands the change to reporting — a Power BI "
        "refresh for VaR/Stress, a dbt rebuild trigger via Control-M for "
        "Sensitivity/FRTB. Every step is recorded: who submitted, who "
        "approved, when it processed, and when the reports picked it up."))

    _html(_flow([
        ("Submit", "New Adjustment page"),
        ("Approve", "if required / Entity Roll"),
        ("Process", "task polls every 1 min"),
        ("Reports", "Power BI refresh or dbt rebuild queued"),
    ]))

    section_title("Supported Scopes", "bar-chart")
    scope_rows = []
    for name, cfg in SCOPE_CONFIG.items():
        scope_rows.append([
            f'{icon(cfg.get("icon", ""), size=13, color=cfg.get("color", "#333"))} '
            f'<strong>{cfg.get("label", name)}</strong>',
            cfg.get("desc", "—"),
        ])
    _html(_table(["Scope", "Description"], scope_rows))
    _html(_card(
        f'{icon("info", size=13, color=P["info"])} The three FRTB sub-types '
        f'(FRTBSBM, FRTBDRC, FRTBRRAO) are processed by one shared FRTB pipeline. '
        f'Selecting several data scopes on the New Adjustment page (Scaling or Entity Roll) simply '
        f'creates one adjustment per selected scope - three sibling tickets that '
        f'process one after another.', P["info"]))

    section_title("Adjustment Categories", "zap")
    cat_rows = [
        ["<strong>Scaling Adjustment</strong>",
         "Scale, flatten or roll fact-table data by a factor. "
         "<em>Flatten</em> zeroes the selected scope; <em>Scale</em> multiplies "
         "it; <em>Roll</em> carries another COB's adjusted values forward.",
         "No (optional)"],
        ["<strong>Direct Adjustment</strong>",
         "Exact values. <em>VaR, Stress, Sensitivity:</em> paste or upload a "
         "CSV — each row is validated and submitted as its own, independent "
         "adjustment. <em>FRTB, FRTBDRC, FRTBRRAO:</em> upload one file in the "
         "scope's upload template — the whole file is one Direct adjustment; "
         "the COB comes from the file's COBID and a re-upload with the same "
         "COB + Reference <em>replaces</em> the previous submission.",
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
        ["<strong>Home</strong>",
         "Dashboard: KPIs, System Status (follows the COB range), recent "
         "activity, overlap alerts, Current Errors. Once a failure is handled "
         "(retried or accepted), acknowledge it here (Current Errors → "
         "<em>Acknowledge a failure</em>, with a note) so System Status "
         "returns to healthy — the failure stays listed, tagged ACK."],
        ["<strong>New Adjustment</strong>",
         "Create and submit adjustments; request a COB re-open; sign a "
         "re-opened COB off again."],
        ["<strong>Adjustments</strong>",
         "Pipeline status boxes and stage board at the top, then browse "
         "everything with history, filters, and per-row actions (Retry "
         "failed, Delete, Submit for approval, Recall)."],
        ["<strong>Approval Queue</strong>",
         "Approvers approve/reject adjustments and COB sign-off / re-open "
         "requests (4-eyes: never your own)."],
        ["<strong>Sign-Off</strong>",
         "COB sign-off status for everyone; sign off (approval optional) or "
         "request a re-open (approval required); sync from the upstream feed."],
        ["<strong>FRTB Explore</strong>",
         "Browse the <em>official</em> FRTB fact tables (SBM, DRC, RRAO) by "
         "COB, entity, risk class, sensitivity type, book or trade: summary "
         "KPIs, a grid, and a CSV download (max 1,000 rows) whose columns are "
         "exactly the FRTB upload template — edit the file and re-upload it "
         "as a Direct adjustment."],
        ["<strong>Admin</strong>",
         "Restricted: scope config, approvers, page administrators, "
         "notifications, reference."],
        ["<strong>Logs</strong>",
         "Processing runs, activity feed, errors, and the sign-off audit trail."],
        ["<strong>Tasks &amp; Cost</strong>",
         "Serverless task health (runs, failures, duration) and the system's "
         "cost — credits and money per task, with daily and monthly charts."],
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
        f'{icon("layers", size=13, color="#7E22CE")} <strong>Multi-scope FRTB:</strong> '
        f'selecting all three FRTB scopes submits three sibling adjustments - one each for '
        f'FRTBSBM, FRTBDRC and FRTBRRAO. They overlap by design, so the pipeline '
        f'runs them in sequence; the preview shown is the sum across the three.',
        "#7E22CE"))

    section_title("Direct Adjustment (paste or upload CSV)", "list")
    _html(_card(
        "<strong>VaR, Stress, Sensitivity — per row.</strong> Paste or upload "
        "a CSV of exact values (the expected columns are shown on the page). "
        "Each row is validated independently and becomes its own adjustment "
        "— a 200-row file submits 200 separate tickets, each with its own USD "
        "value, not one combined adjustment. There is no reference-based "
        "replacement here: to correct a row, submit a new one or act on the "
        "existing ticket from the <strong>Adjustments</strong> page (Retry, "
        "Delete, Recall)."))
    _html(_card(
        f'{icon("layers", size=13, color="#7E22CE")} <strong>FRTB, FRTBDRC, '
        f'FRTBRRAO — per file.</strong> Upload one file in the scope\'s '
        f'<em>upload template</em> (the same columns the FRTB Explore download '
        f'produces). The whole file is stored as <strong>one</strong> Direct '
        f'adjustment (type <em>Direct</em>): the COB is read-only and taken '
        f'from the file\'s COBID column, and the Reference identifies the '
        f'submission. Uploading a new file with the same <strong>COB + '
        f'Reference</strong> <em>replaces</em> the previous submission — the '
        f'old ticket is marked <em>Replaced</em> and its rows removed. '
        f'Typical workflow: FRTB Explore → download the rows you need → edit '
        f'the values → upload here as a Direct adjustment.', "#7E22CE"))

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
        "<strong>Sign-off applies immediately</strong> on an OPEN/REOPENED "
        "entry (approval is <em>optional</em> — tick the checkbox on the "
        "Sign-Off page to route it via an approver first). "
        "<strong>Re-open always goes through approval</strong> (its checkbox "
        "is ticked and locked by policy): request it on a SIGNED_OFF entry "
        "from the Sign-Off page or the New Adjustment panel, and an approver "
        "who is not the requester actions it in the Approval Queue's <em>COB "
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
         "and VaR Upload rows don't overlap-serialise: per-row Direct "
         "adjustments (VaR/Stress/Sensitivity) are independent (no dedupe "
         "needed); file-based submissions (VaR Upload and FRTB Direct files) "
         "use the COB + Reference as their duplicate control — a re-upload "
         "replaces the previous one."],
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
        ["<strong>Direct</strong> (per row: VaR / Stress / Sensitivity)",
         "One header row = one fact row: the codes on the adjustment resolve "
         "directly to dimension keys (case-insensitive, −1 when blank or "
         "unmatched) and its USD value lands straight in the measure column "
         "— no intermediate line items, no FX conversion."],
        ["<strong>Upload</strong> (VaR Upload and FRTB Direct files)",
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
        ["<strong>Sensitivity, FRTBSBM / DRC / RRAO</strong>", "dbt rebuild via Control-M",
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
        f'the Adjustments page — reports may show stale data '
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
         "Adjustments page → the row shows who it is waiting for. Blocked rows "
         "start automatically when the blocker finishes. If it is genuinely "
         "stranded, contact support with the ADJ id."],
        ["Adjustment shows <strong>Failed</strong>",
         "The processing run hit an error (message shown on the ticket).",
         "Adjustments page → select it → <strong>Retry</strong>. Retries are "
         "safe: anything a failed run wrote is cleaned up first. If it keeps "
         "failing, the error text says why — contact support with the ADJ id. "
         "Once handled (retried or accepted), acknowledge it on Home "
         "(Current Errors → <em>Acknowledge a failure</em>) so System Status "
         "returns to healthy; a retry that fails again re-arms it."],
        ["Adjustment stuck in <strong>Running</strong> for hours",
         "The run that claimed it died (timeout, kill). Big Entity Rolls "
         "legitimately run ~20 min — hours means dead.",
         "Nothing required: the pipeline auto-resets it to Failed after 4 "
         "hours and frees anything queued behind it; then Retry from the "
         "Adjustments page."],
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
         "Adjustments page → the pipeline status boxes and the row's report "
         "status show whether the refresh is queued, running or ready."],
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
    # status_badge = the exact badge (colour + icon) used on the Adjustments
    # grid, so the reference matches what users see elsewhere.
    srows = [[status_badge(name), desc] for name, desc in status_desc.items()]
    _html(_table(["Status", "Meaning"], srows))

    section_title("Key Database Objects", "database")
    _html(_table(["Object", "Role"], [
        ["<code>ADJ_HEADER</code>",
         "Single entry point for every adjustment — one row per ticket, with "
         "status, claim token, and blocking info."],
        ["<code>ADJ_LINE_ITEM_JSON</code>",
         "VaR Upload and FRTB Direct file rows (verbatim CSV payload per row)."],
        ["<code>ADJ_STATUS_HISTORY</code>", "Append-only audit of every status transition."],
        ["<code>ADJUSTMENTS_SETTINGS</code>",
         "Per-scope config: fact/adjustment/summary tables, metric columns, "
         "surrogate key definition, active flag."],
        ["<code>ADJ_SIGNOFF_STATUS</code> / <code>ADJ_SIGNOFF_HISTORY</code>",
         "Sign-off lifecycle per COB + entity + scope ('*' = whole scope) + its audit trail."],
        ["<code>ADJ_APPROVERS</code> / <code>ADJ_SIGNOFF_USERS</code> / <code>ADJ_ADMINS</code> / <code>ADJ_CATEGORY</code>",
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
         "Maintainer escape hatch (run directly in Snowflake) — pushes one "
         "stuck adjustment through now; refuses to bypass a live blocker."],
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
