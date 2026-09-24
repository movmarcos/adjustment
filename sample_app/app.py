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
"""
import streamlit as st

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
