"""Which PROCESS_TYPE values count as one sign-off scope.

Marcos, 2026-09-25: "if FRTB is signed off you are considering FRTBRRAO and
FRTBDRC. Now they have their own sign-off process, they can be split. FRTB
and FRTBSBM are the same. FRTBDRC and FRTBRRAO are now separated."

So there are two rules, and they are not the same rule:

  * **FRTB and FRTBSBM are two spellings of ONE scope.** The app's code is
    FRTB (shown as FRTBSBM); an upstream feed row spelled either way signs
    off the same thing.
  * **FRTBDRC and FRTBRRAO are scopes of their own.** An FRTB sign-off says
    nothing about them, and theirs says nothing about FRTB. Before this,
    the New Adjustment page treated an upstream FRTB row as covering all
    three, so a signed-off SBM blocked a DRC adjustment that nobody had
    signed off.

Matching is widened by aliases only, never by family: nothing here may ever
make one scope's sign-off block a different scope.

The stored procedures cannot import this module (they run inside Snowflake),
so 03_sp_submit_adjustment.sql, 14_sp_submit_direct_batch.sql and
10_sp_signoff_sync.sql each carry the same map, and
tests/app/test_signoff_scope_split.py asserts all four agree.
"""

#: Spellings that mean the same scope. Keys and values are upper case.
SCOPE_ALIASES = {
    "FRTB":    ("FRTB", "FRTBSBM"),
    "FRTBSBM": ("FRTB", "FRTBSBM"),
}


def feed_process_types(scope) -> tuple:
    """Every PROCESS_TYPE spelling that counts as `scope`.

    'FRTB' → ('FRTB', 'FRTBSBM'); 'FRTBDRC' → ('FRTBDRC',). An unknown
    scope matches only itself — the safe default, since a scope nobody has
    aliased must not inherit another scope's sign-off.
    """
    s = str(scope or "").strip().upper()
    return SCOPE_ALIASES.get(s, (s,))


def sql_in_list(scope) -> str:
    """The scope's spellings as a SQL IN-list body: "'FRTB', 'FRTBSBM'".

    Values come from SCOPE_ALIASES above, never from the caller, so there is
    nothing here to escape — but the assertion keeps it that way if someone
    later adds an alias with a quote in it.
    """
    names = feed_process_types(scope)
    assert all("'" not in n and "\\" not in n for n in names), names
    return ", ".join(f"'{n}'" for n in names)
