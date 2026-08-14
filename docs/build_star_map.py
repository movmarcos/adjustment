"""Generate docs/RaptorStarMap.html from RaptorReporting.bim.

Re-run after model changes:  python3 docs/build_star_map.py
"""
import json, re, html, collections, pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
BIM = ROOT / "RaptorReporting.bim"
OUT = ROOT / "docs" / "RaptorStarMap.html"

doc = json.loads(BIM.read_text(encoding="utf-8"))
tables = {t["name"]: t for t in doc["model"]["tables"]}
rels = doc["model"]["relationships"]


def is_fact(t):
    return (
        t["partitions"][0].get("mode") == "directQuery"
        or t.get("refreshPolicy")
        or len(t["partitions"]) > 1
    )


def source_obj(t):
    exprs = []
    se = (t.get("refreshPolicy") or {}).get("sourceExpression")
    if se:
        exprs.append(se if isinstance(se, str) else "\n".join(se))
    for p in t.get("partitions", []):
        e = p.get("source", {}).get("expression")
        if e:
            exprs.append(e if isinstance(e, str) else "\n".join(e))
    for x in exprs:
        m = re.findall(r'Name="([A-Za-z0-9_]+)",\s*Kind="(?:View|Table)"', x)
        m = [n for n in m if n not in ("FACT", "DIMENSION", "METADATA", "REPORT")]
        if m:
            return m[0]
    return ""


facts_all = {n for n, t in tables.items() if is_fact(t)}
groups = {
    "VaR": sorted(n for n in facts_all if n.startswith(("VaR", "VAR_"))),
    "Stress": sorted(n for n in facts_all if n.startswith("Stress")),
    "Sensitivity": sorted(n for n in facts_all if n.startswith("Sensitivity")),
}
used = set().union(*[set(v) for v in groups.values()])
groups["Limits, FRTB & Operations"] = sorted(facts_all - used)

GROUP_INTRO = {
    "VaR": """
<p>Daily Value-at-Risk at book and trade grain. The star follows the model's standard
<strong>hot-window pattern</strong>: recent close-of-business dates are imported for fast visuals,
while DirectQuery twins over the <code>*_COMBINED</code> views reach the full history and
trade-level drill-through. <em>VaR Book Import</em> is the workhorse (27 measures hang off it);
the <em>Adjustment</em> twin carries user adjustments from the adjustment engine and pairs with the
original via the <em>IsAdjustment</em> dimension (Original vs Adjustment). <em>MAX</em> and
<em>CYCLIC</em> are the max-window and cyclic-window VaR variants; <em>VAR_SUMMARY_REPORT</em> keeps
12 months at report grain for trends. Most displayed numbers come from the dedicated measure tables
<em>VaR Measures</em> (43), <em>VaR Measures Default</em> (8) and
<em>Market Risk Measure Summary</em> (52), not from the facts themselves.</p>""",
    "Stress": """
<p>Stress-testing P&amp;L per scenario. Scenario identity lives in the
<em>Stress Simulation</em> / <em>Stress Properties</em> / <em>Stress Report Group</em> dimensions,
so one fact row is a (COB, book, scenario) result. Same hot-window pattern as VaR: three import
tables (base, <em>Adjustment</em> twin, <em>Cyclic</em> variant) hold 15 days; the Combined DQ
tables reach history; <em>Stress Summary PBI Report</em> keeps a 1-year summary for trend pages.
<em>Stress CVA</em>, <em>Stress Historic</em> (business-line history) and <em>Stress Profile</em>
are special-purpose DirectQuery views. The 142-measure <em>Stress Measures</em> table is the
display layer for this star.</p>""",
    "Sensitivity": """
<p>Sensitivities (greeks) at two grains. <strong>Summary</strong> (65-day window) is
position-level; <strong>Detail</strong> (32-day window) adds tenor / curve / underlying-tenor
resolution — hence this star's extra dimensions (<em>Curve</em>, <em>Tenor</em>, <em>Skew</em>,
<em>Correlation</em>, rating / spread / beta bands). Each grain has an <em>Adjustment</em> twin,
a DirectQuery <code>*_COMBINED</code> twin for full history, and a 24-month <em>Fields</em>
aggregate for longer trend reporting. This is the widest star in the model (33 shared dimensions)
and the heaviest on refresh — the 65/32-day hot windows are the ones under discussion with the
business.</p>""",
    "Limits, FRTB & Operations": """
<p>Everything that is not one of the three risk stars, in four clusters.
<strong>Limits &amp; utilisation</strong>: <em>Limit Department</em>,
<em>MarketRiskMeasureDepartment</em>, <em>Instrument Measure</em> and <em>Aging Inventory</em> —
department-level measures against limits, on longer 1&ndash;2-year windows.
<strong>FRTB</strong>: <em>FRTBSA Measures</em> and <em>FRTB Instrument</em>, fully DirectQuery.
<strong>Sign-off &amp; workflow</strong>: the <em>PUBLISH*_COMPLETED</em> flags gate "data ready"
status per COB, <em>PowerBI Processing</em> / <em>Pending Adjustments</em> surface pipeline state,
<em>BAMComment</em> / <em>BAMSignOff</em> carry the BAM approval workflow, and the
<em>CV / Cash Adjustment Status</em> views are sign-off controls for adjustments.
<strong>Versioned reference</strong>: <em>Tenor</em>, <em>TenorGroupMapping</em>,
<em>Window Element</em> and <em>Adjustment</em> are COB-versioned reference tables — they carry a
refresh policy like a fact but serve the other stars as dimensions (which is why most of this
model's inactive relationships live here).</p>""",
}

FACT_NOTES = {
    "VaR Book Import": "Main VaR fact at book grain; drives most VaR visuals",
    "VaR Adjustment Summary Import": "Adjustment twin — VaR after user adjustments",
    "VAR_SUMMARY_IMPORT_MAX": "Max-window VaR variant",
    "VAR_SUMMARY_IMPORT_CYCLIC": "Cyclic-window VaR variant",
    "VAR_SUMMARY_REPORT": "12-month report-grain history for trends",
    "VaR Book Instrument DQ": "History / instrument drill beyond the imported window",
    "VaR Trade Direct Query": "Trade-level drill-through",
    "VaR Trade Adhoc DQ": "Ad-hoc trade-level analysis",
    "VaR Measures Summary Adhoc": "Ad-hoc summary analysis",
    "Stress Measures Import": "Main stress fact — (COB, book, scenario) results",
    "Stress Measures Adjustment Import": "Adjustment twin — stress after user adjustments",
    "Stress Measures Import Cyclic": "Cyclic-window stress variant",
    "Stress Summary PBI Report": "1-year summary for trend pages",
    "Stress Measures Combined DQ": "History drill beyond the imported window",
    "Stress Measures Summary Combined DQ": "Summary-grain history drill",
    "Stress Measures Cyclic DQ": "Cyclic-variant history drill",
    "Stress CVA": "CVA stress summary",
    "Stress Historic": "Historic stress by business line",
    "Stress Profile": "Stress profile view",
    "Sensitivity Summary Import": "Position-level sensitivities, hot window",
    "Sensitivity Summary Adjustment Import": "Adjustment twin (summary grain)",
    "Sensitivity Summary DQ": "Summary-grain full history",
    "Sensitivity Summary Fields": "24-month field-level aggregate for trends",
    "Sensitivity Detail Import": "Tenor/curve-level sensitivities, hot window",
    "Sensitivity Detail Adjustment Import": "Adjustment twin (detail grain)",
    "Sensitivity Detail DQ": "Detail-grain full history",
    "Sensitivity Detail Fields": "24-month field-level aggregate (detail grain)",
    "Limit Department": "Department limits and utilisation",
    "MarketRiskMeasureDepartment": "Department-level risk measures vs limits",
    "Instrument Measure": "Instrument-level measures",
    "Aging Inventory": "Inventory aging",
    "FRTBSA Measures": "FRTB standardised-approach measures (incl. adjustments)",
    "FRTB Instrument": "FRTB instrument attributes",
    "PUBLISHVAR_COMPLETED": "Publish gate — VaR data ready per COB",
    "PUBLISHSTRESS_COMPLETED": "Publish gate — stress data ready per COB",
    "PUBLISHSENSITIVITY_COMPLETED": "Publish gate — sensitivity data ready per COB",
    "PowerBI Processing": "Pipeline processing state",
    "Pending Adjustments": "Adjustments awaiting processing",
    "BAMComment": "BAM workflow comments",
    "BAMSignOff": "BAM workflow sign-off state",
    "CV Adjustment VaR": "Sign-off control — VaR adjustment status",
    "CV Adjustment Stress": "Sign-off control — stress adjustment status",
    "Cash Adjustment Status": "Sign-off control — cash adjustment status",
    "Hypo IPV": "Hypothetical IPV",
    "Market Value": "Market value from sensitivity feed",
    "Adjustment": "COB-versioned adjustment reference (dimension for all stars)",
    "Tenor": "COB-versioned tenor reference (dimension for Sensitivity)",
    "TenorGroupMapping": "Tenor-group weighting map, COB-versioned",
    "UnderlyingTenorGroupMapping": "Role-playing twin of TenorGroupMapping (underlying leg)",
    "Underlying Tenor": "Role-playing twin of Tenor (underlying leg)",
    "Window Element": "VaR window / PnL-vector element reference",
}


def mode_chip(t):
    mode = t["partitions"][0].get("mode", "import")
    rp = t.get("refreshPolicy")
    if mode == "directQuery":
        return '<span class="chip dq">DirectQuery</span>'
    if rp:
        win = f'{rp["rollingWindowPeriods"]}{rp["rollingWindowGranularity"][0]}'
        hot = f'{rp["incrementalPeriods"]}{rp["incrementalGranularity"][0]}'
        return f'<span class="chip imp">Import {win} / hot {hot}</span>'
    if mode == "dual":
        return '<span class="chip dual">Dual</span>'
    return '<span class="chip imp">Import</span>'


def dim_chip(name):
    t = tables.get(name)
    if not t:
        return ""
    mode = t["partitions"][0].get("mode", "import")
    src = t["partitions"][0].get("source", {}).get("type")
    if mode == "dual":
        return "dual"
    if mode == "directQuery":
        return "DQ"
    if src == "calculated":
        return "calc"
    return "import"


# ---- build edges per group -------------------------------------------------
group_data = {}
for gname, facts in groups.items():
    cell = collections.defaultdict(list)
    dimcount = collections.Counter()
    edges = []
    for r in rels:
        f, t = r["fromTable"], r["toTable"]
        if f in facts or t in facts:
            fact, dim = (f, t) if f in facts else (t, f)
            e = {
                "fact": fact,
                "dim": dim,
                "fromCol": r.get("fromColumn"),
                "toCol": r.get("toColumn"),
                "active": r.get("isActive", True),
                "bidi": r.get("crossFilteringBehavior") == "bothDirections",
            }
            edges.append(e)
            cell[(dim, fact)].append(e)
    for (dim, fact) in cell:
        dimcount[dim] += 1
    group_data[gname] = {
        "facts": facts,
        "edges": edges,
        "cell": cell,
        "dims": sorted(dimcount, key=lambda x: (-dimcount[x], x)),
        "dimcount": dimcount,
    }

# ---- common dimensions view ------------------------------------------------
dim_groups = collections.defaultdict(dict)  # dim -> {group: fact-count}
for gname, d in group_data.items():
    for dim, n in d["dimcount"].items():
        if dim not in facts_all:  # a fact appearing as a dim row stays in its star
            dim_groups[dim][gname] = n
shared = {d: g for d, g in dim_groups.items() if len(g) >= 2}
shared_order = sorted(
    shared, key=lambda d: (-len(shared[d]), -sum(shared[d].values()), d)
)
gnames = list(groups)
common_rows = []
for dim in shared_order:
    cells = []
    for g in gnames:
        n = shared[dim].get(g)
        cells.append(f'<td class="n">{n}</td>' if n else "<td></td>")
    full = ' class="all"' if len(shared[dim]) == len(gnames) else ""
    common_rows.append(
        f'<tr{full}><th class="dh">{html.escape(dim)}'
        f'<em>{dim_chip(dim)}</em></th>{"".join(cells)}</tr>'
    )
exclusive = sorted(d for d, g in dim_groups.items() if len(g) == 1)
n_conformed = sum(1 for d in shared if len(shared[d]) == len(gnames))

common_section = f"""
<section id="common-dimensions">
  <h2>Common dimensions</h2>
  <p class="stat">{len(shared)} dimensions shared by two or more scopes ·
  {n_conformed} conformed across all four (highlighted)</p>
  <p class="intro">These are the model's <strong>conformed dimensions</strong> — a slicer on any
  of them filters every star it connects to, which is what lets one report page mix VaR, stress
  and sensitivity visuals consistently. Numbers show how many fact tables the dimension serves in
  each scope. <em>Book</em>, <em>Entity</em>, <em>Reporting Date</em> and the adjustment pair
  (<em>Adjustment</em>, <em>IsAdjustment</em>) are the backbone: they touch everything, so a
  change to their keys or granularity ripples across the whole model.</p>
  <div class="scroll">
    <table>
      <thead><tr><th class="corner">dimension ↓ &nbsp;&nbsp; scope →</th>
      {"".join(f'<th class="gh">{html.escape(g)}</th>' for g in gnames)}</tr></thead>
      <tbody>{"".join(common_rows)}</tbody>
    </table>
  </div>
  <details>
    <summary>Scope-exclusive dimensions ({len(exclusive)})</summary>
    <pre>{html.escape(chr(10).join(f"{d}  ({list(dim_groups[d])[0]})" for d in exclusive))}</pre>
  </details>
</section>"""

# ---- star sections ---------------------------------------------------------
def slug(s):
    return "".join(c if c.isalnum() else "-" for c in s.lower())


sections = []
nav = ['<a href="#common-dimensions">Common dimensions</a>']
for gname, d in group_data.items():
    gid = slug(gname)
    nav.append(f'<a href="#{gid}">{html.escape(gname)}</a>')

    fact_rows = "".join(
        f"<tr><th>{html.escape(f)}</th>"
        f"<td>{mode_chip(tables[f])}</td>"
        f'<td class="n">{len(tables[f]["partitions"])}</td>'
        f'<td class="n">{len(tables[f].get("measures", []))}</td>'
        f"<td><code>{html.escape(source_obj(tables[f]))}</code></td>"
        f'<td class="note">{html.escape(FACT_NOTES.get(f, ""))}</td></tr>'
        for f in d["facts"]
    )

    head = "".join(f'<th class="fh"><span>{html.escape(f)}</span></th>' for f in d["facts"])
    rows = []
    for dim in d["dims"]:
        tds = []
        for f in d["facts"]:
            edges = d["cell"].get((dim, f))
            if not edges:
                tds.append("<td></td>")
                continue
            marks = []
            for e in edges:
                tip = f'{e["fact"]}[{e["fromCol"]}] → {e["dim"]}[{e["toCol"]}]'
                if e["bidi"]:
                    cls, tip = "b", tip + " · bi-directional"
                elif not e["active"]:
                    cls, tip = "i", tip + " · inactive (USERELATIONSHIP)"
                else:
                    cls = "a"
                marks.append(f'<span class="c {cls}" title="{html.escape(tip)}"></span>')
            tds.append(f'<td>{"".join(marks)}</td>')
        rows.append(
            f'<tr><th class="dh">{html.escape(dim)}'
            f'<em>{d["dimcount"][dim]}</em></th>{"".join(tds)}</tr>'
        )

    n_bidi = sum(e["bidi"] for e in d["edges"])
    n_inact = sum(not e["active"] for e in d["edges"])
    stat = (
        f'{len(d["facts"])} facts · {len(d["dims"])} dimensions · '
        f'{len(d["edges"])} relationships'
        + (f' · <span class="warn">{n_inact} inactive</span>' if n_inact else "")
        + (f' · <span class="warn">{n_bidi} bi-directional</span>' if n_bidi else "")
    )
    all_tables = "\n".join(d["facts"] + d["dims"])
    sections.append(f"""
<section id="{gid}">
  <h2>{html.escape(gname)}</h2>
  <p class="stat">{stat}</p>
  <div class="intro">{GROUP_INTRO[gname]}</div>
  <div class="scroll facts">
    <table>
      <thead><tr><th>fact table</th><th>storage</th><th>parts</th><th>meas.</th>
      <th>Snowflake source</th><th>role</th></tr></thead>
      <tbody>{fact_rows}</tbody>
    </table>
  </div>
  <div class="scroll">
    <table>
      <thead><tr><th class="corner">dimension ↓ &nbsp;&nbsp; fact →</th>{head}</tr></thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
  </div>
  <details>
    <summary>Table list for the TE3 diagram — <code>{html.escape(gname)}.te3diag</code></summary>
    <pre>{html.escape(all_tables)}</pre>
  </details>
</section>""")

page = f"""<title>RaptorReporting — Star Schema Map</title>
<style>
:root {{
  --bg: #f5f7f6; --surface: #ffffff; --ink: #1c2826; --muted: #5f716d;
  --line: #d9e1df; --accent: #17685e; --accent-soft: #e2efec;
  --amber: #a66a1e; --hollow: #8fa19d;
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --bg: #111817; --surface: #182120; --ink: #d9e3e0; --muted: #8fa19c;
    --line: #2a3634; --accent: #57b3a6; --accent-soft: #1d2f2c;
    --amber: #d9a050; --hollow: #5f716d;
  }}
}}
:root[data-theme="dark"] {{
  --bg: #111817; --surface: #182120; --ink: #d9e3e0; --muted: #8fa19c;
  --line: #2a3634; --accent: #57b3a6; --accent-soft: #1d2f2c;
  --amber: #d9a050; --hollow: #5f716d;
}}
:root[data-theme="light"] {{
  --bg: #f5f7f6; --surface: #ffffff; --ink: #1c2826; --muted: #5f716d;
  --line: #d9e1df; --accent: #17685e; --accent-soft: #e2efec;
  --amber: #a66a1e; --hollow: #8fa19d;
}}
html {{ background: var(--bg); }}
body {{
  font: 15px/1.55 "Avenir Next", "Segoe UI", system-ui, sans-serif;
  color: var(--ink); background: var(--bg);
  max-width: 1160px; margin: 0 auto; padding: 40px 28px 80px;
}}
h1 {{ font-size: 27px; font-weight: 600; letter-spacing: -0.015em; margin: 0 0 6px; text-wrap: balance; }}
.sub {{ color: var(--muted); margin: 0 0 22px; max-width: 62ch; }}
h2 {{ font-size: 19px; font-weight: 600; margin: 52px 0 4px; letter-spacing: -0.01em; }}
.stat {{ color: var(--muted); font-size: 13px; margin: 0 0 10px; }}
.stat .warn {{ color: var(--amber); font-weight: 600; }}
.intro {{ max-width: 76ch; font-size: 14px; margin: 0 0 16px; }}
.intro p {{ margin: 0 0 8px; }}
p.intro {{ margin-bottom: 16px; }}
nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 10px; }}
nav a {{
  color: var(--accent); text-decoration: none; font-size: 13px; font-weight: 600;
  padding: 5px 12px; border: 1px solid var(--line); border-radius: 999px; background: var(--surface);
}}
nav a:hover, nav a:focus-visible {{ background: var(--accent-soft); outline: none; }}
.howto {{
  background: var(--surface); border: 1px solid var(--line); border-radius: 10px;
  padding: 16px 20px; margin: 18px 0 6px; font-size: 14px;
}}
.howto ol {{ margin: 8px 0 0; padding-left: 20px; }}
.howto li {{ margin: 3px 0; }}
.legend {{ display: flex; flex-wrap: wrap; gap: 20px; align-items: center;
  font-size: 13px; color: var(--muted); margin: 14px 0 4px; }}
.legend span {{ display: inline-flex; align-items: center; gap: 7px; }}
.scroll {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 10px;
  background: var(--surface); margin: 0 0 14px; }}
table {{ border-collapse: collapse; font-variant-numeric: tabular-nums; }}
th, td {{ border-top: 1px solid var(--line); }}
thead th {{ border-top: none; }}
td {{ min-width: 34px; height: 30px; text-align: center; }}
td.n {{ font-size: 12px; color: var(--muted); }}
tbody tr:hover td, tbody tr:hover .dh {{ background: var(--accent-soft); }}
.corner {{ font-size: 11px; font-weight: 500; color: var(--muted); text-align: left;
  padding: 8px 12px; vertical-align: bottom; }}
.gh {{ font-size: 12px; font-weight: 600; padding: 8px 14px; }}
.fh {{ vertical-align: bottom; padding: 10px 4px 8px; }}
.fh span {{
  writing-mode: vertical-rl; transform: rotate(180deg);
  font: 12px/1.2 ui-monospace, "SF Mono", Menlo, monospace;
  max-height: 210px; display: inline-block; white-space: nowrap;
}}
.dh {{
  position: sticky; left: 0; background: var(--surface); text-align: left;
  font: 12px/1.3 ui-monospace, "SF Mono", Menlo, monospace; font-weight: 500;
  padding: 6px 12px; white-space: nowrap;
}}
.dh em {{ font-style: normal; color: var(--muted); margin-left: 8px; font-size: 10px; }}
tr.all .dh, tr.all td {{ background: var(--accent-soft); }}
.c {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin: 0 1px; }}
.c.a {{ background: var(--accent); }}
.c.b {{ background: var(--amber); border-radius: 2px; transform: rotate(45deg); }}
.c.i {{ background: transparent; border: 2px solid var(--hollow); width: 7px; height: 7px; }}
.facts table {{ width: 100%; font-size: 13px; }}
.facts th, .facts td {{ text-align: left; padding: 6px 12px; }}
.facts thead th {{ font-size: 11px; font-weight: 600; color: var(--muted);
  text-transform: uppercase; letter-spacing: 0.05em; }}
.facts tbody th {{ font: 12px/1.3 ui-monospace, "SF Mono", Menlo, monospace;
  font-weight: 500; white-space: nowrap; }}
.facts td.n {{ text-align: right; }}
.facts code {{ font: 11px ui-monospace, "SF Mono", Menlo, monospace; color: var(--muted); }}
.facts .note {{ color: var(--muted); }}
.chip {{ display: inline-block; font-size: 11px; font-weight: 600; padding: 1px 8px;
  border-radius: 999px; white-space: nowrap; }}
.chip.imp {{ background: var(--accent-soft); color: var(--accent); }}
.chip.dq {{ border: 1px solid var(--line); color: var(--muted); }}
.chip.dual {{ border: 1px solid var(--accent); color: var(--accent); }}
details {{ margin: 10px 0 0; font-size: 13px; }}
summary {{ cursor: pointer; color: var(--accent); font-weight: 600; }}
summary code {{ font: 12px ui-monospace, "SF Mono", Menlo, monospace; }}
details pre {{
  background: var(--surface); border: 1px solid var(--line); border-radius: 8px;
  padding: 12px 16px; font: 12px/1.6 ui-monospace, "SF Mono", Menlo, monospace;
  overflow-x: auto; column-count: 3; column-gap: 32px; white-space: pre-line;
}}
@media (max-width: 700px) {{ details pre {{ column-count: 1; }} }}
</style>

<h1>RaptorReporting — star schema map</h1>
<p class="sub">Scopes, fact tables and fact-to-dimension relationships of the semantic model.
Generated from <code>RaptorReporting.bim</code> ({len(tables)} tables,
{len(rels)} relationships) by <code>docs/build_star_map.py</code>. Hover a matrix mark to see
the join columns.</p>

<nav>{"".join(nav)}</nav>

<div class="howto">
  <strong>Recreate each star as a diagram in Tabular Editor 3</strong>
  <ol>
    <li>File ▸ New ▸ Diagram.</li>
    <li>In the TOM Explorer, select the tables listed under the matrix (click first, Shift/Ctrl-click the rest) and drag them into the diagram — relationship lines draw automatically.</li>
    <li>Auto-arrange, then Ctrl+S and save as <code>&lt;Star&gt;.te3diag</code> next to the .bim — commit it to git; diagrams reference tables by name, so the same file works on dev and prod.</li>
  </ol>
</div>

<p class="legend">
  <span><span class="c a"></span>active relationship</span>
  <span><span class="c b"></span>bi-directional — review candidate</span>
  <span><span class="c i"></span>inactive — needs USERELATIONSHIP in DAX</span>
  <span>count after each dimension = facts it serves</span>
</p>

{common_section}
{"".join(sections)}
"""

OUT.write_text(page, encoding="utf-8")
print(f"written {OUT} ({len(page)} chars)")
