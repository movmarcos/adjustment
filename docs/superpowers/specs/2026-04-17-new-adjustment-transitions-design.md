# New Adjustment — Transitions & Wait-State UX

**Date:** 2026-04-17
**Scope:** `streamlit_app/pages/1_New_Adjustment.py`
**Status:** Approved for planning

## Problem

Two transitions in the New Adjustment wizard feel slow and opaque:

1. **Continue → Preview** (step 1 → step 2) — button is instant, but step 2's
   render synchronously calls `SP_PREVIEW_ADJUSTMENT`, so the user stares at a
   blank region while Snowflake runs. Worse: every subsequent rerun on step 2
   (clicking *Show Breakdown*, *Show Sample Rows*, expanding the debug panel)
   re-calls the same SP because nothing is cached.
2. **Submit Adjustment** (step 2 → step 3) — a single opaque `st.spinner`
   covers multiple distinct phases (payload build, pandas unpivot, temp-table
   write, SP call). The user has no signal for which phase is active or why
   it's taking time.

## Goals

- Replace both opaque waits with staged progress UX the user can read.
- Surface per-phase timings to diagnose which step is actually slow.
- Eliminate the unnecessary re-runs of `SP_PREVIEW_ADJUSTMENT` caused by
  ordinary step-2 interactions.

## Non-Goals

- No changes to stored-procedure bodies (server side).
- No changes to `_build_payload` logic or validation rules.
- No rewrite of `_write_var_upload_line_items` internals. The existing
  `iterrows` → temp-table → `INSERT SELECT` flow works well enough; leave it.
- No broader visual redesign. Match the existing card/emoji style.

## Design

### 1. Staged `st.status` — Submit flow

Replace the `st.spinner("Submitting adjustment…")` block at the end of step 2
with an `st.status` container that walks explicit phases. Each phase records
`time.perf_counter()` start/end and emits a line like
`✓ Writing line items · 2.3s` as it completes. Container auto-collapses on
success; stays expanded on error with a red status label.

**Phases — VaR Upload:**
1. Validating payload
2. Unpivoting CSV *(N rows × 21 measures)*
3. Resolving VAR_SUB_COMPONENT codes
4. Writing M line items to Snowflake
5. Calling SP_SUBMIT_ADJUSTMENT

**Phases — Scaling / Entity Roll:**
1. Validating payload
2. Calling SP_SUBMIT_ADJUSTMENT

Implementation approach: `_do_submit()` gains one optional parameter,
`status_cb: Callable[[str], None] | None = None`. At each phase boundary it
calls `status_cb("Writing line items")` etc. The caller in step 2 owns the
`st.status` container and passes a callback that appends a line to it.
Default `None` keeps `_do_submit` callable without Streamlit context (e.g.
from tests). Per-phase timing is computed inside `_do_submit` and included
in the callback message (`"Writing line items · 2.3s"`).

### 2. Staged `st.status` — Preview flow

Wrap the `SP_PREVIEW_ADJUSTMENT` call in step 2 (non-broad-scope branch only)
in `st.status("Loading impact preview…", expanded=True)` with phases:
1. Building request
2. Querying Snowflake
3. Computing metrics

Auto-collapses on success. The existing "No matching rows" info message and
"Preview not available" warning paths continue to work; on error the status
container surfaces the exception and stays expanded.

### 3. Preview cache

Add a session-scoped cache keyed on the `preview_json` payload:

```python
wiz["_preview_cache"] = {
    "key": <stable hash of preview_json>,
    "df":  <pandas DataFrame>,
}
```

- Before calling the SP, compute the key and check the cache.
- On hit, skip the SP call and the `st.status` render, reuse the DataFrame.
- Invalidate on **Back** (step 2 → 1) and on `reset_wizard()`.

Net effect: clicking *Show Breakdown*, *Show Sample Rows*, or toggling the
debug expander no longer re-hits Snowflake. The preview SP runs **once per
unique filter combination**, not once per Streamlit rerun.

### 4. What stays exactly the same

- SP bodies.
- `_build_payload()` logic.
- `_write_var_upload_line_items` internals.
- The `🔍 Debug — request params` expander.
- All existing layout, cards, chips, metrics, and navigation buttons.

## Files Touched

Only `streamlit_app/pages/1_New_Adjustment.py`. No new modules, no utility
changes, no SP changes.

## Risks & Mitigations

- **`st.status` availability** — requires Streamlit ≥ 1.27. The app already
  uses modern features; verify before implementation. If unavailable, fall
  back to a manually-styled container with incremental `st.write` lines.
- **Cache staleness** — the cache key hashes `preview_json`, so any filter
  change produces a new key. The only way to serve stale data is to mutate
  `wiz` filter fields without updating `preview_json`, which does not happen
  today — `preview_json` is rebuilt inline from `wiz` on every render.
- **Timing overhead** — `perf_counter` is effectively free; no concern.
