"""Line probe — a bare Streamlit app that imports NOTHING from this repo.

Purpose (2026-09-24): a faint horizontal line crosses every page of the
Adjustment Engine, sidebar included, stays put while the page scrolls, and is
still there with the engine's whole stylesheet switched off. Another app on the
same account and browser does not show it; a second app built from this
codebase does.

This app is the control. It has no repo imports, no theme file, no CSS of its
own until you tick a box. Deploy it next to the engine and open it in the same
browser:

  * Line visible with EVERY box unticked  -> it is the platform / runtime,
                                             not the engine's code.
  * Line appears only after ticking a box -> that box names the cause.
  * No line under any combination         -> it is in the engine's code and
                                             the next step is to bisect there.

Each box adds ONE thing the engine also does. Nothing else changes.

Round 1 result (2026-09-24): NO line, under every box. Platform, runtime,
dark sidebar, wide layout and hidden header are all cleared.

Round 2: the engine's theme file (.streamlit/config.toml) is now shipped with
this app, byte-identical. It is the one thing Streamlit applies to its shell
BEFORE the script runs, and the engine's line is visible during start-up
before anything has been drawn. Nothing else was added.

Round 2 result: the line APPEARED. The theme file is the cause.

Round 3: deploy.py now creates one sibling app per theme setting
(LINE_PROBE_T1_BASE … T5_TEXT), each with a config.toml holding exactly one
line of the engine's theme. The sidebar caption prints which settings the
running app carries. The sibling that shows the line names the setting.

Round 3 result: ALL FIVE siblings showed the line. No colour setting is the
cause; having a theme (or a config file) at all is.

Round 4: T6 a config file with no sections, T7 an empty [theme] section,
T8 a [client] section only — these split "file present" from "theme
present". T9 and T10 carry the whole engine theme plus one candidate fix
each (ui.hideTopBar, client.toolbarMode = "minimal").

Round 4 result: T6 and T8 no line; T7 (empty [theme]) line; T9/T10 line.
An empty [theme] section changes nothing inside Streamlit 1.50, so the line
is Snowsight's reaction to the [theme] header in the file, drawn outside the
app's iframe. Locally on 1.50.0 with the same theme there is no line.

Round 5: T11 carries the engine theme as TOML dotted keys (theme.base = …)
with no [theme] header — the same config to Streamlit, no header for
Snowsight to see.

Round 5 result: T11 has the line. Snowsight parses the TOML properly; any
theme table in the file triggers it.

Round 6: T12 has NO config file. The theme is set from the script (block at
the top of this file, enabled by runtime_theme.flag). The reference app
LINE_PROBE keeps the theme file.
"""
import os

import streamlit as st
from streamlit import config as _st_config

# Round 6: the engine theme set FROM THE SCRIPT, with no config file at all.
# Streamlit rebuilds the theme message from the live config at the start of
# every script run (runtime/app_session.py, SCRIPT_STARTED →
# _create_new_session_message → _populate_theme_msg), so options set here
# reach the front-end on the next run. The first run of the process sets
# them and reruns once; every later session gets the theme from its first
# message. Snowsight, which reads .streamlit/config.toml to pick a colour
# scheme, has nothing to read. Enabled by a marker file the deploy uploads,
# so the same app.py serves the reference app (theme file) and this one.
_RUNTIME_THEME = os.path.exists(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "runtime_theme.flag"))
_THEME = {
    "theme.base": "light",
    "theme.primaryColor": "#D50032",
    "theme.backgroundColor": "#F6F7F9",
    "theme.secondaryBackgroundColor": "#FFFFFF",
    "theme.textColor": "#0F172A",
}
if _RUNTIME_THEME and _st_config.get_option("theme.base") != "light":
    for _k, _v in _THEME.items():
        _st_config.set_option(_k, _v)
    st.rerun()

# Layout must be chosen before set_page_config, so the "wide" choice is read
# from the previous run's checkbox and takes effect on the rerun it triggers.
_wide = bool(st.session_state.get("probe_wide", False))
st.set_page_config(
    page_title="Line probe",
    page_icon="📏",
    layout="wide" if _wide else "centered",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.header("Line probe")
    st.caption(f"Streamlit {st.__version__}")
    # Which theme settings THIS deployment carries — so the round-3 sibling
    # apps (one setting each) identify themselves on screen.
    _keys = ("base", "primaryColor", "backgroundColor",
             "secondaryBackgroundColor", "textColor")
    _set = [f"{k}={st.get_option('theme.' + k)}" for k in _keys
            if st.get_option("theme." + k) is not None]
    st.caption("Theme: " + (", ".join(_set) or "none (bare)"))
    st.write("Tick one box at a time. Note when the line appears.")
    dark = st.checkbox("1 · Dark sidebar", key="probe_dark",
                       help="The engine's sidebar is dark. A faint line is "
                            "invisible on a light one, so tick this first — "
                            "it only changes the background colour.")
    st.checkbox("2 · Wide layout", key="probe_wide",
                help="The engine uses layout='wide'. Takes effect on rerun.")
    nohdr = st.checkbox("3 · Hide Streamlit header", key="probe_nohdr",
                        help="The engine hides Streamlit's top header bar.")
    st.divider()
    st.write("Rows in the main area let you check whether the line moves "
             "when the page scrolls.")

css = []
if dark:
    css.append('[data-testid="stSidebar"]{background:#1B1F26 !important}'
               '[data-testid="stSidebar"] *{color:#E2E5EA !important}')
if nohdr:
    css.append('header[data-testid="stHeader"]{display:none !important}')
if css:
    st.markdown("<style>" + "".join(css) + "</style>", unsafe_allow_html=True)

st.title("Line probe")
st.write(
    "A control app with **no code from the Adjustment Engine**. "
    "Look for a thin horizontal line that crosses the sidebar and this area "
    "at the same height and does not move when you scroll."
)
st.info(
    "Boxes ticked now: "
    + (", ".join(n for n, on in (("dark sidebar", dark),
                                 ("wide layout", _wide),
                                 ("header hidden", nohdr)) if on) or "none")
)

st.subheader("Scroll test")
st.write("Scroll down. If the line stays at the same place on the screen while "
         "these numbers move under it, it is not part of the page.")
for i in range(1, 61):
    st.write(f"Row {i:02d}")
