# Streamlit 1.50 compatibility review — 2026-09-15 (pre-prod)

Method: static audit of every Streamlit call in `streamlit_app/` (app, pages,
utils) against the real 1.50.0 signatures, plus a check of every `data-testid`
the CSS relies on against the 1.50 frontend bundle, plus a scan for widgets
that carry an explicit default while their value is also set via
session_state (1.50 shows an in-app warning for that).

## Fixed in this pass
| Area | Symptom on 1.50 | Fix |
|---|---|---|
| Home charts | Plotly kwargs deprecation warning | `use_container_width=True` (7b0034e) |
| Tasks & Cost charts | `altair_chart() got an unexpected keyword argument 'width'` | `use_container_width=True` (51f2606) |
| Sidebar | Home entry rendered blank | override 1.50's hidden `::after` on the link text (1b10c72) |
| Section cards (New Adjustment etc.) | `stVerticalBlockBorderWrapper` test-id no longer exists → cards lose the MUFG look | `bordered_container()` keys each card (`st-key-card<n>`), CSS targets that class; ≤1.49 selector kept |
| Pills CSS | `stPills` test-id renamed | selector also covers `stButtonGroup` (st.pills is not used today) |
| Adjustments filters | "Clear filters" / `?status=` deep link → yellow "widget created with a default value but also had its value set via Session State" | widgets no longer pass `default=[]` / `value=False` |

## Verified OK
- No Streamlit call passes a keyword its 1.50 signature lacks (charts were the
  only offenders). `wide_kwargs()` targets (dataframe, buttons, data_editor)
  all accept `width` on 1.50.
- `st.experimental_rerun` / `st.experimental_get_query_params` appear only in
  `except AttributeError` / `hasattr` fallbacks that never run on 1.50.
- `st.user`, `st.rerun`, `st.query_params`, `st.cache_data`, `st.toggle`,
  `st.page_link`, `st.switch_page`, `st.column_config`, `st.data_editor`
  all exist on 1.50.
- Button and alert test-ids (`stBaseButton-*`, `stAlertContent*`) are built
  dynamically in the bundle and still match the CSS.

## Not covered by a static audit (check once on the deployed app)
- Visual pass of every page in the browser (card padding, sidebar spacing).
- `render_activity_grid` still uses the selectbox picker rather than native
  row selection; not a break, a UX gap.
- Widgets with computed keys (`_k(...)` in New Adjustment) could not be checked
  for the default-vs-session_state warning statically; the helpers there
  are documented as avoiding it.
