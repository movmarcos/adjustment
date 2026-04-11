"""
Snowflake Connection Utilities
===============================
Provides a unified way to get a Snowflake session, whether running
inside Streamlit in Snowflake (SiS) or externally.
"""

import streamlit as st


def get_session():
    """
    Get a Snowflake session.

    - Inside SiS: uses get_active_session()
    - External: uses st.connection() with Snowpark connector
    """
    # Try SiS first (native Streamlit in Snowflake)
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except (ImportError, Exception):
        pass

    # Fallback: external Streamlit with snowflake-connector
    # Requires [connections.snowflake] in .streamlit/secrets.toml
    try:
        conn = st.connection("snowflake")
        return conn.session()
    except Exception as e:
        st.error(
            f"Cannot connect to Snowflake: {e}\n\n"
            "If running externally, configure `.streamlit/secrets.toml`:\n"
            "```toml\n"
            "[connections.snowflake]\n"
            'account = "your_account"\n'
            'user = "your_user"\n'
            'password = "your_password"\n'
            'warehouse = "ADJUSTMENT_WH"\n'
            'database = "ADJUSTMENT_DB"\n'
            'schema = "CORE"\n'
            "```"
        )
        st.stop()


@st.cache_resource
def get_cached_session():
    """Returns a cached Snowflake session (singleton per app lifecycle)."""
    return get_session()
