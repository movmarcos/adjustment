"""Business-readable descriptions of a Direct upload's validation rules.

Marcos, 2026-09-25: "describe the rule in English where business can
understand". The rules live in three places; this module covers the two
that can be rendered from data with no model involved, so they can never
drift from what the engine enforces:

  * the column contract (EXPECTED_COLUMNS + ALIASES in DIRECT_SCOPE_SCHEMA)
  * the conditional "required when" rules (VALIDATION_RULES, same table),
    whose shape is  {"field": F, "conditions": [[COL, REGEX, NEGATE], ...],
    "error": "..."}  — every condition must hold for the field to be required.

The third place — the SQL views (VW_DIRECT_VALIDATE and the enriched
views) — is described by Cortex and cached in ADJ_RULE_DOCS; this module
only formats that stored text (see rules_html).

Everything here is pure: no Streamlit, no session, so it is tested directly.
"""
import html
import re
from collections import OrderedDict

# Column names → the words the business uses for them. Anything not listed
# is shown as Title Case of the column name with underscores as spaces.
FIELD_WORDS = {
    "RISK_CLASS":       "Risk Class",
    "SENSITIVITY_TYPE": "Sensitivity Type",
    "TRADE_CODE":       "Trade Code",
    "BOOK_CODE":        "Book Code",
    "ENTITY_CODE":      "Entity Code",
    "CURVE_TYPE":       "Curve Type",
}

# Regex alternatives → business words. Longer patterns first so that
# "NON.*SEC.*CREDIT" is matched before "SEC".
VALUE_WORDS = OrderedDict([
    ("NON.*SEC.*CREDIT", "Non-Sec (Credit)"),
    ("NON.*SEC.*EQUITY", "Non-Sec (Equity)"),
    ("NON.*SEC",         "Non-Sec"),
    ("EQUIT",            "Equity"),
    ("CURVATURE",        "Curvature"),
    ("VEGA",             "Vega"),
    ("DELTA",            "Delta"),
    ("GIRR",             "GIRR"),
    ("CSR",              "CSR"),
    ("FX",               "FX"),
    ("SEC",              "Sec"),
])

_EMPTY_PATTERNS = {"^(|NAN|NONE)$", "^$", ""}


def field_words(column: str) -> str:
    col = str(column or "").strip().upper()
    if col in FIELD_WORDS:
        return FIELD_WORDS[col]
    return col.replace("_", " ").title()


def _value_words(pattern: str) -> str:
    """'FX|GIRR' → 'FX or GIRR'; 'EQUIT|CSR' → 'Equity or CSR'."""
    parts = [p.strip() for p in str(pattern).split("|") if p.strip()]
    words = []
    for p in parts:
        words.append(VALUE_WORDS.get(p, VALUE_WORDS.get(p.upper(), p)))
    if not words:
        return str(pattern)
    if len(words) == 1:
        return words[0]
    return ", ".join(words[:-1]) + " or " + words[-1]


def condition_text(cond) -> str:
    """One [column, regex, negate] triple → 'Risk Class is FX or GIRR'."""
    try:
        col, pattern, negate = cond[0], cond[1], bool(cond[2]) if len(cond) > 2 else False
    except (TypeError, IndexError):
        return str(cond)
    name = field_words(col)
    if str(pattern).strip() in _EMPTY_PATTERNS:
        return f"{name} is {'not ' if negate else ''}empty"
    return f"{name} is {'not ' if negate else ''}{_value_words(pattern)}"


def rule_when(rule) -> str:
    conds = rule.get("conditions") or []
    if not conds:
        return "always"
    return " and ".join(condition_text(c) for c in conds)


def describe_rules(rules) -> list:
    """Group VALIDATION_RULES by their condition, in first-seen order.

    Returns [{"when": "Risk Class is FX or GIRR", "fields": ["CCY1", ...],
              "errors": ["CCY1 is required for ..."]}, ...]. The grouping is
    the whole point: eleven scattered rules become a few readable blocks.
    """
    groups = OrderedDict()
    for rule in rules or []:
        if not isinstance(rule, dict) or not rule.get("field"):
            continue
        when = rule_when(rule)
        g = groups.setdefault(when, {"when": when, "fields": [], "errors": []})
        f = str(rule["field"]).strip().upper()
        if f not in g["fields"]:
            g["fields"].append(f)
        err = str(rule.get("error") or "").strip()
        if err and err not in g["errors"]:
            g["errors"].append(err)
    return list(groups.values())


def describe_columns(expected, aliases=None) -> list:
    """[{"name", "required", "type", "alias"}] for the column contract.

    ALIASES is stored as business header → canonical name; here it is shown
    the other way round, next to the canonical column it maps to.
    """
    by_canonical = {}
    for alias, canonical in (aliases or {}).items():
        by_canonical.setdefault(str(canonical).strip().upper(), []).append(
            str(alias).strip().upper())
    rows = []
    for c in expected or []:
        if isinstance(c, dict):
            name = str(c.get("name") or "").strip().upper()
            if not name:
                continue
            rows.append({"name": name,
                         "required": bool(c.get("required")),
                         "type": str(c.get("type") or "string"),
                         "alias": ", ".join(by_canonical.get(name, []))})
        elif c:
            name = str(c).strip().upper()
            rows.append({"name": name, "required": False, "type": "string",
                         "alias": ", ".join(by_canonical.get(name, []))})
    return rows


# ── HTML ─────────────────────────────────────────────────────────────────────

def _e(s) -> str:
    return html.escape(str(s if s is not None else ""))


def _table(headers, rows) -> str:
    th = "".join(f'<th style="text-align:left;padding:4px 10px;border-bottom:1px solid var(--border);'
                 f'font-size:0.74rem;text-transform:uppercase;letter-spacing:.04em;'
                 f'color:var(--ink-2)">{_e(h)}</th>' for h in headers)
    trs = []
    for r in rows:
        tds = "".join(f'<td style="padding:4px 10px;border-bottom:1px solid var(--border);'
                      f'font-size:0.82rem;vertical-align:top">{c}</td>' for c in r)
        trs.append(f"<tr>{tds}</tr>")
    return (f'<div style="overflow-x:auto"><table style="border-collapse:collapse;width:100%">'
            f'<thead><tr>{th}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>')


def _h(text) -> str:
    return (f'<div style="font-weight:700;font-size:0.9rem;margin:0.9rem 0 0.35rem">'
            f'{_e(text)}</div>')


def _fields_html(fields) -> str:
    return " ".join(
        f'<code style="font-size:0.78rem;padding:1px 6px;border:1px solid var(--border);'
        f'border-radius:6px;background:var(--card)">{_e(f)}</code>' for f in fields)


def rules_html(scope_label: str, columns: list, groups: list, generic=None) -> str:
    """The whole panel as one HTML block.

    generic: None (nothing stored yet) or {"text": str, "stale": bool,
    "reviewed": bool, "generated_at": str, "model": str} from ADJ_RULE_DOCS.
    """
    parts = [f'<div style="font-size:0.85rem;color:var(--ink-2)">These are the checks '
             f'a <strong>{_e(scope_label)}</strong> upload has to pass before it is '
             f'accepted. Rows that fail are listed with the reason, and nothing is '
             f'written until every row passes.</div>']

    # 1. Columns
    if columns:
        req = [c for c in columns if c["required"]]
        opt = [c for c in columns if not c["required"]]
        parts.append(_h("Columns"))
        parts.append(f'<div style="font-size:0.82rem;margin-bottom:0.3rem">'
                     f'{len(req)} always required · {len(opt)} optional or required by '
                     f'the rules below. Column order and letter case do not matter.</div>')
        rows = []
        for c in req + opt:
            rows.append([f'<strong>{_e(c["name"])}</strong>' if c["required"] else _e(c["name"]),
                         "Always" if c["required"] else "See rules",
                         _e(c["type"]),
                         _e(c["alias"]) if c["alias"] else
                         '<span style="color:var(--ink-3)">—</span>'])
        parts.append(_table(["Column", "Required", "Type", "Also accepted as"], rows))

    # 2. Required when …
    parts.append(_h("Required depending on the row"))
    if groups:
        rows = [[f'<strong>{_e(g["when"])}</strong>', _fields_html(g["fields"])] for g in groups]
        parts.append(_table(["When", "These columns must be filled"], rows))
    else:
        parts.append('<div style="font-size:0.82rem;color:var(--ink-2)">No row-dependent '
                     'rules for this scope.</div>')

    # 3. Every row must … (from ADJ_RULE_DOCS)
    parts.append(_h("Every row must also pass"))
    if generic and (generic.get("text") or "").strip():
        badges = []
        if generic.get("stale"):
            badges.append('<span style="background:#FEF3C7;color:#92400E;border:1px solid #FDE68A;'
                          'border-radius:999px;padding:1px 8px;font-size:0.72rem;font-weight:700">'
                          'RULES CHANGED SINCE THIS WAS WRITTEN</span>')
        if not generic.get("reviewed"):
            badges.append('<span style="background:var(--card);color:var(--ink-2);border:1px solid var(--border);'
                          'border-radius:999px;padding:1px 8px;font-size:0.72rem;font-weight:700">'
                          'DRAFT — not yet reviewed</span>')
        meta = []
        if generic.get("generated_at"):
            meta.append(f'written {_e(generic["generated_at"])}')
        if generic.get("reviewed") and generic.get("reviewed_by"):
            meta.append(f'reviewed by {_e(generic["reviewed_by"])}')
        parts.append('<div style="margin-bottom:0.35rem">' + " ".join(badges)
                     + (f' <span style="font-size:0.74rem;color:var(--ink-3)">{" · ".join(meta)}</span>'
                        if meta else "") + '</div>')
        parts.append(_markdownish(generic["text"]))
    else:
        parts.append('<div style="font-size:0.82rem;color:var(--ink-2)">Not written yet — an '
                     'admin can generate this on the Admin page (Business Rules).</div>')
    return '<div style="line-height:1.5">' + "".join(parts) + "</div>"


def _markdownish(text: str) -> str:
    """The stored description is asked for as '- ' bullets; render those as a
    list and anything else as paragraphs. Escaped: it is model output."""
    items, paras, out = [], [], []

    def _flush_items():
        nonlocal items
        if items:
            out.append('<ul style="margin:0.2rem 0 0.4rem 1.1rem;padding:0;font-size:0.84rem">'
                       + "".join(f"<li>{_inline(i)}</li>" for i in items) + "</ul>")
            items = []

    for raw in str(text).splitlines():
        line = raw.strip()
        if not line:
            _flush_items()
            continue
        m = re.match(r"^[-*•]\s+(.*)$", line)
        if m:
            items.append(m.group(1))
        else:
            _flush_items()
            out.append(f'<div style="font-size:0.84rem;margin:0.2rem 0">{_inline(line)}</div>')
    _flush_items()
    return "".join(out)


def _inline(s: str) -> str:
    """Escape, then allow **bold** and `code` only."""
    t = _e(s)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
    return t
