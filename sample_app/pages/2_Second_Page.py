"""A second page so the sidebar has the multipage navigation the engine has.

Streamlit draws its own separator under the page list; this page exists so
that separator is present in the control app too.
"""
import streamlit as st

st.title("Second page")
st.write("Nothing here. This page exists so the sidebar shows a page list, "
         "exactly as the engine's does.")
