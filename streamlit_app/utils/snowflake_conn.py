"""
Snowflake Connection — Streamlit on Snowflake (SiS)
====================================================
When running on SiS, the session is injected automatically.
When running locally, reads from connection.toml or env vars.
"""
import streamlit as st


def safe_rerun():
    """Version-compatible rerun — works on both SiS and local Streamlit."""
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def get_session():
    """Return a Snowpark Session. Cached per Streamlit session."""
    if "snowpark_session" not in st.session_state:
        try:
            # Streamlit in Snowflake — session injected by the platform
            from snowflake.snowpark.context import get_active_session
            _sess = get_active_session()
            try:
                _sess.sql("USE WAREHOUSE DVLP_RAVEN_WH_M").collect()
            except Exception:
                pass
            st.session_state["snowpark_session"] = _sess
        except Exception:
            # Local development — use connection parameters
            from snowflake.snowpark import Session
            conn = st.connection("snowflake")
            st.session_state["snowpark_session"] = Session.builder.configs({
                "account":   conn._connect_params.get("account", ""),
                "user":      conn._connect_params.get("user", ""),
                "password":  conn._connect_params.get("password", ""),
                "warehouse": conn._connect_params.get("warehouse", ""),
                "database":  conn._connect_params.get("database", "DVLP_RAPTOR_NEWADJ"),
                "schema":    conn._connect_params.get("schema", "ADJUSTMENT_APP"),
            }).create()
    return st.session_state["snowpark_session"]


def run_query(sql: str):
    """Run SQL and return list of Row objects."""
    return get_session().sql(sql).collect()


def run_query_df(sql: str):
    """Run SQL and return a pandas DataFrame."""
    return get_session().sql(sql).to_pandas()


def call_procedure(proc_name: str, *args):
    """Call a stored procedure and return the result."""
    args_str = ", ".join([
        f"'{a}'" if isinstance(a, str) else str(a) for a in args
    ])
    return get_session().sql(f"CALL {proc_name}({args_str})").collect()


def call_sp_df(proc_name: str, *args):
    """Call a tabular stored procedure using session.call() and return a pandas DataFrame.

    Uses session.call() (not session.sql("CALL ...")) which correctly handles
    RETURNS TABLE() procedures in all Snowpark runtime versions.
    """
    import pandas as pd
    try:
        return get_session().call(proc_name, *args).to_pandas()
    except Exception:
        # Fallback: SQL CALL with manual Row→dict conversion
        args_str = ", ".join(f"'{a}'" if isinstance(a, str) else str(a) for a in args)
        rows = get_session().sql(f"CALL {proc_name}({args_str})").collect()
        if not rows:
            return pd.DataFrame()
        # Build DataFrame from field names on the first row
        fields = list(rows[0]._fields) if hasattr(rows[0], "_fields") else list(rows[0].as_dict().keys())
        return pd.DataFrame([list(r) for r in rows], columns=fields)


def current_user_name() -> str:
    """Get the logged-in user identity.

    In SiS, CURRENT_USER() returns the *viewer's* identity (not the app
    owner) provided the owner role has been granted READ SESSION:
        GRANT READ SESSION ON ACCOUNT TO ROLE <owner_role>;
    This is the primary and most reliable method.

    Fallback: st.experimental_user / st.user for edge cases.
    """
    # 1. Primary — SQL CURRENT_USER()  (works in SiS with READ SESSION grant)
    try:
        row = get_session().sql("SELECT CURRENT_USER() AS U").collect()
        if row and row[0]["U"]:
            return str(row[0]["U"])
    except Exception:
        pass

    # 2. Fallback — st.experimental_user  (SiS warehouse runtime)
    try:
        eu = getattr(st, "experimental_user", None)
        if eu is not None:
            name = eu.get("user_name") or eu.get("email")
            if name:
                return str(name)
    except Exception:
        pass

    # 3. Fallback — st.user  (newer SiS runtimes)
    try:
        u = getattr(st, "user", None)
        if u is not None:
            name = u.get("user_name") or u.get("email")
            if name:
                return str(name)
    except Exception:
        pass

    return "unknown"
