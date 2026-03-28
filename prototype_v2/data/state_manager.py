"""
State Manager — in-memory database via st.session_state
========================================================
Single source of truth for all adjustments, queue, and config.
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from copy import deepcopy

from data.mock_data import (
    SCOPES, FACT_GENERATORS, USERS, SEED_ADJUSTMENTS,
    SEED_STATUS_HISTORY, SEED_QUEUE, BUSINESS_DATES, LATEST_COB,
)

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

VALID_TRANSITIONS = {
    "DRAFT":            ["PENDING_APPROVAL", "CANCELLED"],
    "PENDING_APPROVAL": ["APPROVED", "REJECTED", "DRAFT"],
    "APPROVED":         ["APPLIED", "PENDING_APPROVAL"],
    "APPLIED":          ["REVERSED"],
    "REJECTED":         [],
    "REVERSED":         [],
    "CANCELLED":        [],
}

STATUS_COLORS = {
    "DRAFT":            "#78909C",
    "PENDING_APPROVAL": "#FB8C00",
    "APPROVED":         "#1976D2",
    "APPLIED":          "#388E3C",
    "REJECTED":         "#D32F2F",
    "REVERSED":         "#7B1FA2",
    "CANCELLED":        "#9E9E9E",
}

STATUS_ICONS = {
    "DRAFT":            "✏️",
    "PENDING_APPROVAL": "⏳",
    "APPROVED":         "✅",
    "APPLIED":          "✔️",
    "REJECTED":         "❌",
    "REVERSED":         "↩️",
    "CANCELLED":        "🚫",
}

TYPE_LABELS = {
    "FLATTEN": ("Flatten", "Zero out all matching positions"),
    "SCALE":   ("Scale",   "Multiply all matching positions by a factor"),
    "ROLL":    ("Roll",    "Copy forward from a prior COB date"),
    "COPY":    ("Copy COB","Duplicate a prior COB with optional scaling"),
}

TYPE_FORMULA = {
    "FLATTEN": "new = original × 0  → delta = -original",
    "SCALE":   "new = original × f  → delta = original × (f - 1)",
    "ROLL":    "new = prior_day × f → delta = prior_day × f - original",
    "COPY":    "Insert new records from source COB × f (target date must have no data)",
}

# ──────────────────────────────────────────────────────────────────────────────
# INIT
# ──────────────────────────────────────────────────────────────────────────────

def init_state():
    if st.session_state.get("_sm_initialized"):
        return

    # Fact tables (one per scope)
    st.session_state["fact_tables"] = {
        sk: gen() for sk, gen in FACT_GENERATORS.items()
    }

    # Adjustments dict: adj_id → dict
    st.session_state["adjustments"] = {
        a["adj_id"]: deepcopy(a) for a in SEED_ADJUSTMENTS
    }

    # Status history list
    st.session_state["status_history"] = deepcopy(SEED_STATUS_HISTORY)

    # Processing queue list
    st.session_state["queue"] = deepcopy(SEED_QUEUE)

    # Auto-increment counters
    st.session_state["_next_adj_id"]   = max(a["adj_id"] for a in SEED_ADJUSTMENTS) + 1
    st.session_state["_next_queue_id"] = max(q["queue_id"] for q in SEED_QUEUE) + 1
    st.session_state["_next_hist_id"]  = max(h["history_id"] for h in SEED_STATUS_HISTORY) + 1

    # Current user (default: first admin user)
    st.session_state["current_user"] = USERS[0]

    # Wizard state (reset on each new adjustment)
    _reset_wizard()

    st.session_state["_sm_initialized"] = True


def _reset_wizard():
    st.session_state["wizard"] = {
        "step": 1,
        "scope_key": None,
        "adj_type": None,
        "frequency": "ADHOC",
        "target_date": LATEST_COB,
        "start_cob": BUSINESS_DATES[-5],
        "end_cob": LATEST_COB,
        "scale_factor": 1.0,
        "roll_source_date": BUSINESS_DATES[-2] if len(BUSINESS_DATES) >= 2 else None,
        "filter_criteria": {},
        "business_reason": "",
        "ticket_reference": "",
        "submit_for_approval": False,
        "preview_data": None,
        "overlaps": [],
        "matched_rows": 0,
    }


def reset_wizard():
    _reset_wizard()

# ──────────────────────────────────────────────────────────────────────────────
# USER HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def current_user() -> dict:
    return st.session_state.get("current_user", USERS[0])


def set_current_user(user_id: str):
    u = next((u for u in USERS if u["id"] == user_id), USERS[0])
    st.session_state["current_user"] = u


def can_approve() -> bool:
    return current_user()["role"] in ("ADJ_APPROVER", "ADJ_ADMIN")


def can_apply() -> bool:
    return current_user()["role"] in ("ADJ_APPROVER", "ADJ_ADMIN", "ADJ_OPERATOR")


def can_admin() -> bool:
    return current_user()["role"] == "ADJ_ADMIN"

# ──────────────────────────────────────────────────────────────────────────────
# FACT TABLE QUERIES
# ──────────────────────────────────────────────────────────────────────────────

def query_fact(scope_key: str, filter_criteria: dict,
               target_date: date | None = None) -> pd.DataFrame:
    """Return fact rows matching the given filters."""
    df = st.session_state["fact_tables"][scope_key].copy()
    if target_date is not None:
        df = df[df["business_date"] == target_date]
    for col, vals in filter_criteria.items():
        if vals:  # empty list = no filter (all values)
            df = df[df[col].isin(vals)]
    return df


def count_matched_rows(scope_key: str, filter_criteria: dict,
                       target_date: date | None = None) -> int:
    return len(query_fact(scope_key, filter_criteria, target_date))

# ──────────────────────────────────────────────────────────────────────────────
# DELTA COMPUTATION
# ──────────────────────────────────────────────────────────────────────────────

def compute_preview(scope_key: str, adj_type: str, filter_criteria: dict,
                    target_date: date, scale_factor: float = 1.0,
                    roll_source_date: date | None = None) -> pd.DataFrame:
    """Return preview dataframe with original/delta/projected columns."""
    scope = SCOPES[scope_key]
    measures = scope["measures"]

    df = query_fact(scope_key, filter_criteria, target_date).copy()
    if df.empty:
        return df

    for m in measures:
        col = m["column"]
        if col not in df.columns:
            continue
        orig = df[col]
        if adj_type == "FLATTEN":
            delta = -orig
        elif adj_type == "SCALE":
            delta = orig * (scale_factor - 1)
        elif adj_type in ("ROLL", "COPY"):
            if roll_source_date is not None:
                src_df = query_fact(scope_key, filter_criteria, roll_source_date)
                src_sum = src_df[col].sum() if not src_df.empty else 0.0
                # Distribute source proportionally (simplified)
                delta = (orig / orig.sum() * src_sum * scale_factor - orig
                         if orig.sum() != 0 else pd.Series(0.0, index=orig.index))
            else:
                delta = pd.Series(0.0, index=orig.index)
        else:
            delta = pd.Series(0.0, index=orig.index)

        df[f"{col}_original"]  = orig
        df[f"{col}_delta"]     = delta.round(2)
        df[f"{col}_projected"] = (orig + delta).round(2)

    return df

# ──────────────────────────────────────────────────────────────────────────────
# OVERLAP DETECTION
# ──────────────────────────────────────────────────────────────────────────────

def _filters_intersect(filters_a: dict, filters_b: dict, scope_key: str) -> bool:
    """
    Two filter sets intersect if, for every dimension, the value sets overlap
    (or one side is empty, meaning 'all values').
    """
    dims = [d["column"] for d in SCOPES[scope_key]["dimensions"]]
    for dim in dims:
        va = set(filters_a.get(dim, []))
        vb = set(filters_b.get(dim, []))
        # Empty = 'all values' → always intersects with anything
        if not va or not vb:
            continue
        if va.isdisjoint(vb):
            return False  # No overlap on this dimension → no overlap at all
    return True


def _date_overlap(adj: dict, target_date: date) -> bool:
    if adj["frequency"] == "ADHOC":
        return adj["target_date"] == target_date
    # Recurring: overlaps if target_date falls in the recurring window
    start = adj.get("start_cob") or adj["target_date"]
    end   = adj.get("end_cob")   or adj["target_date"]
    return start <= target_date <= end


def check_overlaps(scope_key: str, target_date: date, filter_criteria: dict,
                   exclude_adj_id: int | None = None) -> list[dict]:
    """Return list of overlapping adjustments with conflict details."""
    conflicts = []
    non_final = {"DRAFT", "PENDING_APPROVAL", "APPROVED", "APPLIED"}

    for adj_id, adj in st.session_state.get("adjustments", {}).items():
        if adj_id == exclude_adj_id:
            continue
        if adj["scope_key"] != scope_key:
            continue
        if adj["adj_status"] not in non_final:
            continue
        if not _date_overlap(adj, target_date):
            continue
        if _filters_intersect(adj["filter_criteria"], filter_criteria, scope_key):
            # Estimate rows in common
            combined = {}
            for dim in [d["column"] for d in SCOPES[scope_key]["dimensions"]]:
                va = set(adj["filter_criteria"].get(dim, []))
                vb = set(filter_criteria.get(dim, []))
                if va and vb:
                    combined[dim] = list(va & vb)
                elif va:
                    combined[dim] = list(va)
                elif vb:
                    combined[dim] = list(vb)
            rows_in_common = count_matched_rows(scope_key, combined, target_date)
            conflicts.append({
                "adj_id": adj_id,
                "adj":    adj,
                "rows_in_common": rows_in_common,
            })
    return conflicts

# ──────────────────────────────────────────────────────────────────────────────
# ADJUSTMENT CRUD
# ──────────────────────────────────────────────────────────────────────────────

def create_adjustment(scope_key: str, adj_type: str, frequency: str,
                      target_date: date, filter_criteria: dict,
                      scale_factor: float = 1.0,
                      roll_source_date: date | None = None,
                      start_cob: date | None = None,
                      end_cob: date | None = None,
                      business_reason: str = "",
                      ticket_reference: str = "",
                      submit_for_approval: bool = False) -> int:
    """Create adjustment + optionally move to PENDING_APPROVAL. Returns adj_id."""
    adj_id = st.session_state["_next_adj_id"]
    st.session_state["_next_adj_id"] += 1

    now = datetime.now()
    user = current_user()["id"]
    affected = count_matched_rows(scope_key, filter_criteria, target_date)

    adj = {
        "adj_id": adj_id,
        "scope_key": scope_key,
        "adj_type": adj_type,
        "adj_status": "DRAFT",
        "frequency": frequency,
        "target_date": target_date,
        "start_cob": start_cob,
        "end_cob": end_cob,
        "scale_factor": scale_factor,
        "roll_source_date": roll_source_date,
        "filter_criteria": deepcopy(filter_criteria),
        "affected_rows": affected,
        "business_reason": business_reason,
        "ticket_reference": ticket_reference,
        "created_by": user,
        "created_at": now,
        "approved_by": None, "approved_at": None,
        "applied_by": None,  "applied_at": None,
        "reversed_by": None, "reversed_at": None,
        "ai_summary": None,
        "queue_status": None,
    }
    st.session_state["adjustments"][adj_id] = adj

    _append_history(adj_id, None, "DRAFT", user, "Adjustment created.")

    if submit_for_approval:
        update_status(adj_id, "PENDING_APPROVAL", "Submitted for approval.")

    return adj_id


def update_status(adj_id: int, new_status: str, comment: str = "") -> dict:
    """Update adjustment status with validation. Returns {success, error}."""
    adj = st.session_state["adjustments"].get(adj_id)
    if adj is None:
        return {"success": False, "error": f"Adjustment #{adj_id} not found."}

    old_status = adj["adj_status"]
    if new_status not in VALID_TRANSITIONS.get(old_status, []):
        return {"success": False,
                "error": f"Transition {old_status} → {new_status} is not allowed."}

    user = current_user()["id"]
    now  = datetime.now()

    # Self-approval guard
    if new_status == "APPROVED" and adj["created_by"] == user and not can_admin():
        return {"success": False, "error": "You cannot approve your own adjustment."}

    adj["adj_status"] = new_status

    if new_status == "APPROVED":
        adj["approved_by"] = user
        adj["approved_at"] = now
    elif new_status == "APPLIED":
        adj["applied_by"] = user
        adj["applied_at"] = now
        _enqueue(adj_id, adj["affected_rows"])
        _generate_ai_summary(adj)
    elif new_status == "REVERSED":
        adj["reversed_by"] = user
        adj["reversed_at"] = now
        _create_reversal(adj_id)

    _append_history(adj_id, old_status, new_status, user, comment)
    return {"success": True, "transition": f"{old_status} → {new_status}"}


def _create_reversal(original_adj_id: int):
    """Create a negating adjustment for a reversal."""
    orig = st.session_state["adjustments"][original_adj_id]
    new_id = st.session_state["_next_adj_id"]
    st.session_state["_next_adj_id"] += 1

    rev = deepcopy(orig)
    rev.update({
        "adj_id": new_id,
        "adj_status": "APPLIED",
        "business_reason": f"REVERSAL of ADJ #{original_adj_id}: {orig['business_reason']}",
        "ticket_reference": orig["ticket_reference"],
        "created_by": current_user()["id"],
        "created_at": datetime.now(),
        "approved_by": current_user()["id"],
        "approved_at": datetime.now(),
        "applied_by": current_user()["id"],
        "applied_at": datetime.now(),
        "reversed_by": None, "reversed_at": None,
        "ai_summary": None, "queue_status": "COMPLETED",
        # Negate scale factor semantics
        "scale_factor": 1.0 / orig["scale_factor"] if orig["scale_factor"] != 0 else 1.0,
    })
    if orig["adj_type"] == "FLATTEN":
        rev["adj_type"] = "SCALE"
        rev["scale_factor"] = -1.0  # actually reversal needs special handling
    st.session_state["adjustments"][new_id] = rev
    _append_history(new_id, None, "APPLIED", current_user()["id"],
                    f"Auto-created reversal of ADJ #{original_adj_id}")


def _append_history(adj_id: int, old_status, new_status: str,
                    changed_by: str, comment: str):
    hist_id = st.session_state["_next_hist_id"]
    st.session_state["_next_hist_id"] += 1
    st.session_state["status_history"].append({
        "history_id": hist_id, "adj_id": adj_id,
        "old_status": old_status, "new_status": new_status,
        "changed_by": changed_by, "changed_at": datetime.now(),
        "comment": comment,
    })

# ──────────────────────────────────────────────────────────────────────────────
# QUEUE MANAGEMENT
# ──────────────────────────────────────────────────────────────────────────────

def _enqueue(adj_id: int, estimated_rows: int):
    q_id = st.session_state["_next_queue_id"]
    st.session_state["_next_queue_id"] += 1
    st.session_state["queue"].append({
        "queue_id": q_id, "adj_id": adj_id,
        "queued_at": datetime.now(), "started_at": None, "completed_at": None,
        "status": "PENDING", "progress": 0,
        "estimated_rows": estimated_rows, "processed_rows": 0,
        "error_message": None, "worker": "TASK_ADJ_PROCESSOR",
    })


def tick_queue():
    """Advance queue items based on elapsed time (call on each render of queue page)."""
    now = datetime.now()
    for item in st.session_state.get("queue", []):
        if item["status"] == "COMPLETED" or item["status"] == "FAILED":
            continue
        elapsed = (now - item["queued_at"]).total_seconds()
        if elapsed < 4:
            item["status"] = "PENDING"
        elif elapsed < 20:
            item["status"] = "RUNNING"
            item["started_at"] = item["started_at"] or (item["queued_at"] + timedelta(seconds=4))
            pct = min(99, (elapsed - 4) / 16 * 100)
            item["progress"] = round(pct)
            item["processed_rows"] = int(item["estimated_rows"] * pct / 100)
        else:
            item["status"] = "COMPLETED"
            item["completed_at"] = item["completed_at"] or now
            item["progress"] = 100
            item["processed_rows"] = item["estimated_rows"]
            # Mark the adjustment as applied if it isn't already
            adj = st.session_state["adjustments"].get(item["adj_id"])
            if adj and adj["adj_status"] == "APPLIED":
                item["queue_status"] = "COMPLETED"


def get_queue_stats() -> dict:
    q = st.session_state.get("queue", [])
    return {
        "pending":   sum(1 for i in q if i["status"] == "PENDING"),
        "running":   sum(1 for i in q if i["status"] == "RUNNING"),
        "completed": sum(1 for i in q if i["status"] == "COMPLETED"),
        "failed":    sum(1 for i in q if i["status"] == "FAILED"),
        "total":     len(q),
    }

# ──────────────────────────────────────────────────────────────────────────────
# AGGREGATE QUERIES
# ──────────────────────────────────────────────────────────────────────────────

def get_all_adjustments() -> list[dict]:
    return list(st.session_state.get("adjustments", {}).values())


def get_my_adjustments() -> list[dict]:
    uid = current_user()["id"]
    return [a for a in get_all_adjustments() if a["created_by"] == uid]


def get_pending_approvals() -> list[dict]:
    return [a for a in get_all_adjustments() if a["adj_status"] == "PENDING_APPROVAL"]


def get_status_history(adj_id: int) -> list[dict]:
    return [h for h in st.session_state.get("status_history", [])
            if h["adj_id"] == adj_id]


def dashboard_kpis() -> dict:
    adjs = get_all_adjustments()
    today_adjs = [a for a in adjs
                  if a.get("created_at") and a["created_at"].date() == date.today()]
    return {
        "total":             len(adjs),
        "draft":             sum(1 for a in adjs if a["adj_status"] == "DRAFT"),
        "pending_approval":  sum(1 for a in adjs if a["adj_status"] == "PENDING_APPROVAL"),
        "approved":          sum(1 for a in adjs if a["adj_status"] == "APPROVED"),
        "applied":           sum(1 for a in adjs if a["adj_status"] == "APPLIED"),
        "rejected":          sum(1 for a in adjs if a["adj_status"] == "REJECTED"),
        "reversed":          sum(1 for a in adjs if a["adj_status"] == "REVERSED"),
        "today":             len(today_adjs),
        "my_pending":        sum(1 for a in get_my_adjustments()
                                if a["adj_status"] in ("DRAFT", "PENDING_APPROVAL")),
        "queue_running":     get_queue_stats()["running"],
        "queue_pending":     get_queue_stats()["pending"],
    }

# ──────────────────────────────────────────────────────────────────────────────
# MOCK AI  (Cortex in production)
# ──────────────────────────────────────────────────────────────────────────────

def generate_ai_copilot(wizard: dict) -> str:
    """Return contextual markdown guidance based on wizard state."""
    scope_key  = wizard.get("scope_key")
    adj_type   = wizard.get("adj_type")
    filters    = wizard.get("filter_criteria", {})
    target_date= wizard.get("target_date")
    scale      = wizard.get("scale_factor", 1.0)
    overlaps   = wizard.get("overlaps", [])
    matched    = wizard.get("matched_rows", 0)

    if not scope_key:
        return ("**👋 Welcome to the AI Co-pilot**\n\n"
                "I'll guide you through each step. Start by selecting a **data source** on the left.\n\n"
                "_I'll provide real-time analysis as you configure the adjustment._")

    scope = SCOPES[scope_key]
    messages = []

    # Step 1 context
    if scope_key == "PNL":
        messages.append(f"**{scope['icon']} P&L Source Selected**\n\n"
                        "MUREX P&L positions. Adjustments here directly affect reported P&L "
                        "and downstream reports. Finance controller sign-off is recommended.")
    elif scope_key == "RISK":
        messages.append(f"**{scope['icon']} Risk Source Selected**\n\n"
                        "CALYPSO sensitivities (Delta, Gamma, Vega, DV01). Changes here affect "
                        "VaR, stress tests, and regulatory capital calculations.")
    elif scope_key == "NOSTRO":
        messages.append(f"**{scope['icon']} Nostro Source Selected**\n\n"
                        "SWIFT cash positions. Typically adjusted for SWIFT delays or reconciliation. "
                        "Treasury Ops should be informed of any material changes.")

    if not adj_type:
        return "\n\n".join(messages)

    # Type explanation
    type_explanations = {
        "FLATTEN": ("**🔴 Flatten — Full Zero-Out**\n\n"
                    "All matched positions will be set to **zero**. This is irreversible without "
                    "a reversal adjustment. Ensure the desk head has confirmed."),
        "SCALE":   (f"**📊 Scale by {scale:.4f}×**\n\n"
                    f"Formula: `new = original × {scale:.4f}`\n\n"
                    f"{'⬆️ Upward scaling — increasing positions.' if scale > 1 else '⬇️ Downward scaling — reducing positions.' if scale < 1 else '⚠️ Factor is 1.0 — no change will occur.'}"
                    + ("\n\n⚠️ Factor > 1.10 — large adjustment, double-check justification." if scale > 1.10 else "")),
        "ROLL":    ("**🔄 Roll Forward**\n\n"
                    "Copies and replaces today's positions with values from the source COB "
                    "(optionally scaled). Use when today's file is missing or delayed."),
        "COPY":    ("**📋 Copy COB**\n\n"
                    "Duplicates all records from source date into the target date. "
                    "Target date must be empty. Used for pre-positioning or what-if analysis."),
    }
    messages.append(type_explanations.get(adj_type, ""))

    # Filter context
    if filters and matched > 0:
        active_dims = [k for k, v in filters.items() if v]
        scope_cfg = SCOPES[scope_key]
        dim_labels = {d["column"]: d["label"] for d in scope_cfg["dimensions"]}
        filter_desc = ", ".join(
            f"{dim_labels.get(k, k)}: {', '.join(v)}"
            for k, v in filters.items() if v
        )
        messages.append(f"**📌 Scope: {matched} records matched**\n\n"
                        f"Active filters: {filter_desc}\n\n"
                        f"{'💡 Tip: Add Currency or Desk filters to narrow the scope further.' if len(active_dims) < 2 else ''}")
    elif filters and matched == 0:
        messages.append("**⚠️ No records match your current filters**\n\n"
                        "Check that the selected dimensions have data on this COB date. "
                        "Try broadening your filters or changing the target date.")

    # Overlap warning
    if overlaps:
        n = len(overlaps)
        overlap_lines = []
        for o in overlaps[:3]:
            a = o["adj"]
            overlap_lines.append(
                f"- **ADJ #{a['adj_id']}** ({a['adj_type']} · {a['adj_status']}) "
                f"by {a['created_by']} — {o['rows_in_common']} rows in common"
            )
        messages.append(
            f"**⚠️ {n} Overlap{'s' if n > 1 else ''} Detected**\n\n"
            + "\n".join(overlap_lines)
            + "\n\nProceed only if double-adjustment is intentional. "
              "Consider coordinating with the other adjustment's owner."
        )

    # Risk estimate
    if matched > 0 and adj_type:
        if matched > 100 or (adj_type == "FLATTEN") or (adj_type == "SCALE" and abs(scale - 1) > 0.1):
            risk = "HIGH"
            risk_note = "Large scope or significant factor change."
        elif matched > 30 or (adj_type == "SCALE" and abs(scale - 1) > 0.05):
            risk = "MEDIUM"
            risk_note = "Moderate scope — ensure documentation is complete."
        else:
            risk = "LOW"
            risk_note = "Narrow scope, routine adjustment."

        risk_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
        messages.append(f"**Risk Classification: {risk_colors[risk]} {risk}**\n\n{risk_note}")

    return "\n\n---\n\n".join(messages)


def _generate_ai_summary(adj: dict):
    """Generate a mock AI summary when an adjustment is applied."""
    scope = SCOPES.get(adj["scope_key"], {})
    t = adj["adj_type"]
    sf = adj.get("scale_factor", 1.0)
    rows = adj["affected_rows"]
    filters = adj["filter_criteria"]
    fc = ", ".join(f"{k}={v}" for k, v in filters.items() if v)

    if t == "FLATTEN":
        action = "zeroed out all"
    elif t == "SCALE":
        pct = (sf - 1) * 100
        action = f"scaled by {sf:.4f}× ({pct:+.1f}%)"
    elif t == "ROLL":
        action = f"rolled forward from {adj.get('roll_source_date', 'prior COB')}"
    else:
        action = f"applied {t}"

    adj["ai_summary"] = (
        f"This {t} adjustment {action} for {rows} records "
        f"({fc}). Scope: {scope.get('full_label', adj['scope_key'])} ({scope.get('source_system', '')}). "
        f"Reason: {adj['business_reason'][:100]}. "
        f"Risk: {'HIGH' if rows > 100 else 'MEDIUM' if rows > 30 else 'LOW'}."
    )


def generate_chat_response(question: str) -> tuple[str, str]:
    """Mock Cortex NL query. Returns (answer, sql_hint)."""
    q = question.lower()
    adjs = get_all_adjustments()

    if "pending" in q or "approval" in q or "waiting" in q:
        pending = get_pending_approvals()
        n = len(pending)
        names = [f"ADJ #{a['adj_id']} by {a['created_by']}" for a in pending[:3]]
        return (f"There are **{n} adjustment(s) pending approval**: {', '.join(names)}.",
                "SELECT * FROM ADJ_HEADER WHERE ADJ_STATUS = 'PENDING_APPROVAL'")

    if "applied today" in q or "today" in q:
        today = [a for a in adjs if a.get("applied_at") and
                 a["applied_at"].date() == date.today()]
        return (f"**{len(today)} adjustments** were applied today.",
                "SELECT COUNT(*) FROM ADJ_HEADER WHERE DATE(APPLIED_AT) = CURRENT_DATE()")

    if "largest" in q or "biggest" in q or "impact" in q:
        applied = [a for a in adjs if a["adj_status"] == "APPLIED"]
        if applied:
            biggest = max(applied, key=lambda x: x["affected_rows"])
            return (f"The largest applied adjustment is **ADJ #{biggest['adj_id']}** "
                    f"affecting **{biggest['affected_rows']} rows** "
                    f"({biggest['adj_type']} on {biggest['scope_key']}).",
                    "SELECT * FROM ADJ_HEADER WHERE ADJ_STATUS='APPLIED' ORDER BY AFFECTED_ROWS DESC LIMIT 1")

    if "rejected" in q or "denial" in q:
        rejected = [a for a in adjs if a["adj_status"] == "REJECTED"]
        return (f"**{len(rejected)} adjustment(s)** have been rejected. "
                f"Common reason: scope too broad or missing justification.",
                "SELECT * FROM ADJ_HEADER WHERE ADJ_STATUS = 'REJECTED'")

    if "recurring" in q or "template" in q or "schedule" in q:
        recur = [a for a in adjs if a.get("frequency") == "RECURRING"]
        return (f"**{len(recur)} recurring adjustment(s)** are configured. "
                f"They run automatically after each COB file arrival.",
                "SELECT * FROM ADJ_HEADER WHERE FREQUENCY = 'RECURRING'")

    if "scope" in q or "source" in q or "pnl" in q or "risk" in q or "nostro" in q:
        from collections import Counter
        counts = Counter(a["scope_key"] for a in adjs)
        lines = [f"- **{k}**: {v} adjustments" for k, v in counts.most_common()]
        return ("Adjustments by source:\n" + "\n".join(lines),
                "SELECT SCOPE_KEY, COUNT(*) FROM ADJ_HEADER GROUP BY SCOPE_KEY")

    if "my" in q or "i " in q:
        mine = get_my_adjustments()
        return (f"You have **{len(mine)} adjustments**. "
                f"{sum(1 for a in mine if a['adj_status']=='APPLIED')} applied, "
                f"{sum(1 for a in mine if a['adj_status']=='PENDING_APPROVAL')} pending.",
                f"SELECT * FROM ADJ_HEADER WHERE CREATED_BY = '{current_user()['id']}'")

    if "anomal" in q or "unusual" in q or "outlier" in q:
        big = [a for a in adjs if a["affected_rows"] > 100]
        ids = ", ".join(f"#{a['adj_id']}" for a in big[:5])
        return (f"**{len(big)} adjustment(s)** have unusually large scope (>100 rows). "
                f"IDs: {ids}. "
                f"These may warrant additional review.",
                "SELECT * FROM ADJ_HEADER WHERE AFFECTED_ROWS > 100 ORDER BY AFFECTED_ROWS DESC")

    return (f"I found **{len(adjs)} adjustments** in total. "
            f"Ask me about pending approvals, applied today, largest impacts, "
            f"rejections, recurring adjustments, or anomalies.",
            "SELECT COUNT(*) FROM ADJ_HEADER")
