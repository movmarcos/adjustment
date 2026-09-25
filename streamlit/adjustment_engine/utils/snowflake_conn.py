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
    # Local dev: config.py is in streamlit/, two levels above utils/.
    import sys
    import pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    import config


def sql_escape(value) -> str:
    """THE escape for a value going into a single-quoted Snowflake literal.

    Doubles backslashes FIRST, then single quotes — that order matters:
    Snowflake interprets `\\` inside a single-quoted literal, so a value
    ending in a backslash escapes the closing quote and breaks out of the
    literal when only the quote is doubled. Returns the literal BODY (no
    surrounding quotes), so call sites keep writing `'{sql_escape(v)}'`.

    None → "" (an empty literal), matching the pages' previous local copies.

    This is the single definition for the whole app: every page used to carry
    its own two-line copy and one of them (the Admin role lookup) had silently
    skipped the backslash step. Import this instead of re-implementing it."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def sql_lit(value) -> str:
    """sql_escape() wrapped in single quotes — for building IN (...) lists."""
    return "'" + sql_escape(value) + "'"


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
    Snowflake-style (backslashes doubled FIRST, then quotes — see sql_escape)."""
    def _lit(a):
        return sql_lit(a) if isinstance(a, str) else str(a)
    args_str = ", ".join(_lit(a) for a in args)
    return get_session().sql(f"CALL {proc_name}({args_str})").collect()


def _sp_call_sql(proc_name: str, *args) -> str:
    """The `CALL proc(arg, …)` text for a stored procedure.

    String args go through sql_escape (backslashes doubled FIRST — Snowflake
    literals interpret \\n, \\t, ... — then quotes), otherwise JSON args with
    newlines or quotes break the literal and the SP's json.loads."""
    def _lit(a):
        return sql_lit(a) if isinstance(a, str) else str(a)
    return f"CALL {proc_name}({', '.join(_lit(a) for a in args)})"


def _rows_to_df(rows):
    """Row objects → pandas DataFrame, column names off the first row."""
    import pandas as pd
    if not rows:
        return pd.DataFrame()
    first = rows[0]
    # `or` (not hasattr): some Row shapes carry _fields = None.
    fields = getattr(first, "_fields", None) or list(first.as_dict().keys())
    return pd.DataFrame([list(r) for r in rows], columns=list(fields))


def call_sp_df(proc_name: str, *args):
    """Call a tabular stored procedure using session.call() and return a pandas DataFrame.

    Uses session.call() (not session.sql("CALL ...")) which correctly handles
    RETURNS TABLE() procedures in all Snowpark runtime versions.
    """
    try:
        return get_session().call(proc_name, *args).to_pandas()
    except Exception:
        # Fallback: SQL CALL with manual Row→dict conversion.
        return _rows_to_df(get_session().sql(_sp_call_sql(proc_name, *args)).collect())


class _EagerSPJob:
    """Already-finished job: holds a DataFrame the caller gathers later.

    The sequential fallback for runtimes without async jobs. The caller's
    submit-then-gather loop reads the same, but the work happened at SUBMIT
    time — so a submit loop here raises where the async one would not."""

    def __init__(self, df):
        self._df = df
        self.is_async = False

    def result_df(self):
        return self._df


class _AsyncSPJob:
    """A running Snowpark AsyncJob; .result_df() blocks until it finishes."""

    def __init__(self, job):
        self._job = job
        self._df = None
        self.is_async = True

    def result_df(self):
        if self._df is None:
            self._df = _rows_to_df(self._job.result())
        return self._df


def call_sp_df_async(proc_name: str, *args):
    """Start a tabular SP call without waiting; returns an object with .result_df().

    Uses Snowpark's async jobs (DataFrame.collect_nowait → AsyncJob) so several
    per-scope procedure calls run CONCURRENTLY in Snowflake — no Python threads,
    no extra sessions. When the runtime does not support async jobs the call is
    executed EAGERLY here instead, at submit time, and the caller's gather loop
    then just reads the stored DataFrame.

    READ-ONLY procedures only: a submit that fails part-way is retried on the
    synchronous path, so a writing procedure (SP_SUBMIT_ADJUSTMENT and friends)
    could run twice. Those must keep using call_sp_df / call_procedure."""
    try:
        job = get_session().sql(_sp_call_sql(proc_name, *args)).collect_nowait()
        if not hasattr(job, "result"):
            # Some runtimes return the rows themselves — nothing async about it.
            raise TypeError("collect_nowait did not return an AsyncJob")
        return _AsyncSPJob(job)
    except Exception:
        return _EagerSPJob(call_sp_df(proc_name, *args))


def gather_dfs(jobs):
    """Collect every submitted job, in submission order."""
    return [j.result_df() for j in jobs]


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


def submit_access(user: str):
    """Who may SUBMIT an adjustment, from ADJ_SUBMITTERS (Admin page).

    Same shape and same bootstrap rule as `signoff_access`: None while the
    list has no active row, meaning everyone may submit; otherwise the set
    of scope codes this user may submit for, with "*" for every scope. An
    empty set means the user is listed nowhere and may not submit.

    Gates SUBMIT only. Everyone keeps the New Adjustment page, the draft and
    the impact preview — checking a number is not the same as booking it.

    Best-effort: if the table cannot be read this returns None and the page
    leaves Submit enabled, because SP_SUBMIT_ADJUSTMENT applies the very
    same rule and is the gate that actually counts.
    """
    try:
        rows_ = get_session().sql("""
            SELECT UPPER(USERNAME) AS U, PROCESS_TYPE AS PT
            FROM ADJUSTMENT_APP.ADJ_SUBMITTERS
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


def can_submit(access, scope) -> bool:
    """True when `access` (from submit_access) allows submitting `scope`."""
    if access is None or "*" in access:
        return True
    return str(scope or "").strip().upper() in access


# ── Direct adjustments in production ────────────────────────────────────────
# Marcos, 2026-09-25: "if it is running in the PROD_RAPTOR database, the
# Direct adjustment should be disabled. I will wait for the user sign-off."
#
# So this is a temporary, reversible gate, not a permanent rule: once the
# business signs Direct adjustments off, an admin sets
# ADJ_APP_CONFIG.DIRECT_ADJUSTMENT_ENABLED = true and it opens, with no
# redeploy and no code change.

DIRECT_ENABLED_KEY = "DIRECT_ADJUSTMENT_ENABLED"
_PROD_PREFIX = "PROD"


def current_database() -> str:
    """The database this app is ACTUALLY running in, upper-cased.

    Asked of Snowflake rather than read from config, and that matters here:
    config.ENV is baked in at deploy time, so a copy deployed by an older
    deploy.py (no deploy_target.py) would claim to be DVLP while running in
    production. Cached per session — an app cannot change database under
    itself. Falls back to config.DATABASE only if the query fails.
    """
    try:
        if st.session_state.get("_current_database"):
            return st.session_state["_current_database"]
    except Exception:
        pass
    db = ""
    try:
        rows = get_session().sql("SELECT CURRENT_DATABASE() AS D").collect()
        db = str(rows[0]["D"] or "").strip().upper() if rows else ""
    except Exception:
        db = ""
    if not db:
        try:
            import config
            db = str(getattr(config, "DATABASE", "") or "").strip().upper()
        except Exception:
            db = ""
    try:
        if db:
            st.session_state["_current_database"] = db
    except Exception:
        pass
    return db


def is_production() -> bool:
    """True when the app runs in a PROD_* database.

    Either signal is enough: the live database name, or the environment
    baked in at deploy time. A gate that protects production must not be
    opened by one of them being unavailable.
    """
    if current_database().startswith(_PROD_PREFIX):
        return True
    try:
        import config
        return str(getattr(config, "ENV", "")).strip().upper() == _PROD_PREFIX
    except Exception:
        return False


def forget_direct_gate() -> None:
    """Drop the cached answer (after an admin changes the setting)."""
    try:
        st.session_state.pop("_direct_gate", None)
    except Exception:
        pass


def direct_adjustment_gate():
    """(enabled, reason) for the Direct Adjustment category.

    Outside production: always enabled. In production: disabled until
    ADJ_APP_CONFIG.DIRECT_ADJUSTMENT_ENABLED is exactly 'true'.

    FAILS CLOSED in production — an unreadable config table leaves Direct
    off. That is the opposite of the submitter list, which fails open, and
    deliberately so: an unreadable permission table should not stop the
    day's work, but an unreadable feature switch must not open a gate whose
    whole purpose is to stay shut until somebody decides otherwise.

    The gate that counts is in SP_SUBMIT_ADJUSTMENT and
    SP_SUBMIT_DIRECT_BATCH; this one disables the category and says why.
    """
    if not is_production():
        return True, ""
    try:
        cached = st.session_state.get("_direct_gate")
        if cached is not None:
            return cached
    except Exception:
        pass
    enabled = False
    try:
        rows = get_session().sql(f"""
            SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
            WHERE CONFIG_KEY = '{DIRECT_ENABLED_KEY}'
        """).collect()
        enabled = bool(rows) and str(rows[0][0] or "").strip().lower() == "true"
    except Exception:
        enabled = False
    answer = (True, "") if enabled else (False, (
        "Direct adjustments are switched off in production while they wait "
        "for user sign-off. Everything else on this page works as usual. "
        "Once the business has signed them off, an admin enables them on "
        "the Admin page (Scope Configuration › Direct adjustments in "
        "production) — no redeploy needed."))
    try:
        st.session_state["_direct_gate"] = answer
    except Exception:
        pass
    return answer


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
