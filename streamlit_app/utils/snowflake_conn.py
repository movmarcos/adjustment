"""
Snowflake Connection — Streamlit on Snowflake (SiS)
====================================================
When running on SiS, the session is injected automatically.
When running locally, reads from connection.toml or env vars.
"""
import streamlit as st

try:
    # In SiS, config.py is shipped to the stage root (on sys.path).
    import config
except ModuleNotFoundError:
    # Local dev: config.py lives at the repo root, two levels above utils/.
    import sys
    import pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    import config


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
                _sess.sql(f"USE WAREHOUSE {config.WAREHOUSE}").collect()
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
                "warehouse": conn._connect_params.get("warehouse", config.WAREHOUSE),
                "database":  conn._connect_params.get("database", config.DATABASE),
                "schema":    conn._connect_params.get("schema", config.SCHEMA),
            }).create()
    return st.session_state["snowpark_session"]


def run_query(sql: str):
    """Run SQL and return list of Row objects."""
    return get_session().sql(sql).collect()


def run_query_df(sql: str):
    """Run SQL and return a pandas DataFrame."""
    return get_session().sql(sql).to_pandas()


def _cached_df_impl():
    """st.cache_data-wrapped reader, built once (cache_data needs ≥1.18;
    older runtimes fall back to the uncached reader)."""
    import streamlit as st
    if not hasattr(st, "cache_data"):
        return run_query_df

    @st.cache_data(ttl=60, show_spinner=False)
    def _read(sql: str):
        return get_session().sql(sql).to_pandas()
    return _read


_CACHED_DF = None


def run_query_df_cached(sql: str):
    """run_query_df with a 60-second server-side cache keyed by the SQL text.

    For page-load reads that every widget interaction reruns (the
    Adjustments page re-executed three queries per keystroke — the picker
    felt broken). Any WHERE that depends on the user is part of the SQL
    string, so the key is user-safe. Actions that change data must call
    bust_query_cache() before rerunning so the page reflects them at once."""
    global _CACHED_DF
    if _CACHED_DF is None:
        _CACHED_DF = _cached_df_impl()
    return _CACHED_DF(sql)


def bust_query_cache():
    """Drop every cached read (call after delete / retry / recall / clone /
    submit, before the rerun)."""
    try:
        import streamlit as st
        if hasattr(st, "cache_data"):
            st.cache_data.clear()
    except Exception:
        pass


def call_procedure(proc_name: str, *args):
    """Call a stored procedure and return the result. String args are escaped
    Snowflake-style (backslashes doubled FIRST, then quotes — see call_sp_df)."""
    def _lit(a):
        if isinstance(a, str):
            return "'" + a.replace("\\", "\\\\").replace("'", "''") + "'"
        return str(a)
    args_str = ", ".join(_lit(a) for a in args)
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
        # Fallback: SQL CALL with manual Row→dict conversion.
        # Double backslashes first (Snowflake literals interpret \n, \t, ...),
        # then double single quotes — otherwise JSON args with newlines or
        # quotes break the literal and the SP's json.loads.
        def _lit(a):
            if isinstance(a, str):
                return "'" + a.replace("\\", "\\\\").replace("'", "''") + "'"
            return str(a)
        args_str = ", ".join(_lit(a) for a in args)
        rows = get_session().sql(f"CALL {proc_name}({args_str})").collect()
        if not rows:
            return pd.DataFrame()
        # Build DataFrame from field names on the first row
        fields = list(rows[0]._fields) if hasattr(rows[0], "_fields") else list(rows[0].as_dict().keys())
        return pd.DataFrame([list(r) for r in rows], columns=fields)


def friendly_error(exc) -> str:
    """Translate common Snowflake error patterns into plain language for
    non-technical users, keeping a short technical tail for support tickets."""
    raw = " ".join(str(exc).split())
    low = raw.lower()
    hints = [
        ("does not exist or not authorized",
         "A database object is missing or access has not been granted — this "
         "is a deployment/permissions issue, not something you did wrong."),
        ("insufficient privileges",
         "Your role does not have permission for this action."),
        ("statement reached its statement or warehouse timeout",
         "The operation hit the warehouse time limit and was stopped."),
        ("timeout", "The operation took too long and was stopped."),
        ("is not recognized",
         "A value has the wrong format for its field (for example text where "
         "a number or date is expected)."),
        ("duplicate key", "This record already exists."),
        ("json", "The data sent to the database was malformed."),
    ]
    for pat, msg in hints:
        if pat in low:
            return f"{msg} (Technical detail: {raw[:180]}…)" if len(raw) > 180 \
                   else f"{msg} (Technical detail: {raw})"
    return (f"The database reported an unexpected error — if it persists, "
            f"contact support with this detail: {raw[:220]}")


def signoff_access(user: str):
    """Who may sign off / request re-open, from ADJ_SIGNOFF_USERS (Admin page).

    Returns None while the list has no active user — bootstrap rule, everyone
    may act (SP_REQUEST_SIGNOFF_CHANGE applies the same rule). Otherwise the
    set of scope codes the user may act on, with "*" meaning every scope; an
    empty set means the user is not listed. Best-effort: if the table cannot
    be read the page falls back to "everyone" and the SP remains the gate."""
    try:
        rows_ = get_session().sql(f"""
            SELECT UPPER(USERNAME) AS U, PROCESS_TYPE AS PT
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_USERS
            WHERE IS_ACTIVE = TRUE
        """).collect()
    except Exception:
        return None
    if not rows_:
        return None
    me = str(user or "").strip().upper()
    scopes = set()
    for r in rows_:
        if str(r["U"] or "").strip().upper() != me:
            continue
        pt = r["PT"]
        if pt is None or not str(pt).strip():
            return {"*"}
        scopes.add(str(pt).strip().upper())
    return scopes


def can_sign_off(access, scope) -> bool:
    """True when `access` (from signoff_access) allows acting on `scope`."""
    if access is None or "*" in access:
        return True
    return str(scope or "").strip().upper() in access


def current_user_name() -> str:
    """Get the logged-in user identity.

    In SiS, CURRENT_USER() returns the *viewer's* identity (not the app
    owner) provided the owner role has been granted READ SESSION:
        GRANT READ SESSION ON ACCOUNT TO ROLE <owner_role>;
    This is the primary and most reliable method.

    Fallback: st.user for edge cases.

    Cached per session: this is called 2-3x on EVERY page render (page top,
    sidebar, helpers) and each rerun — uncached it was 2-3 Snowflake
    round-trips per widget interaction, a large share of perceived page
    latency. Identity cannot change within a session, so caching is safe.
    """
    _cached = st.session_state.get("_current_user_cached")
    if _cached:
        return _cached
    # 1. Primary — SQL CURRENT_USER()  (works in SiS with READ SESSION grant)
    try:
        row = get_session().sql("SELECT CURRENT_USER() AS U").collect()
        if row and row[0]["U"]:
            st.session_state["_current_user_cached"] = str(row[0]["U"])
            return str(row[0]["U"])
    except Exception:
        pass

    # 2. Fallback — st.user (Streamlit ≥ 1.42). Do NOT touch st.experimental_user:
    #    on 1.50 it still exists as a deprecation proxy, and any attribute/key
    #    access on it renders a yellow "Please replace st.experimental_user with
    #    st.user" banner in the app (once per server process). It never raises.
    try:
        u = getattr(st, "user", None)
        if u is not None:
            name = None
            for k in ("user_name", "email", "name"):
                try:
                    name = u.get(k) if hasattr(u, "get") else getattr(u, k, None)
                except Exception:
                    name = None
                if name:
                    break
            if name:
                st.session_state["_current_user_cached"] = str(name)
                return str(name)
    except Exception:
        pass

    return "unknown"
