"""The line-probe control app must stay a control: nothing from this repo.

Marcos, 2026-09-24: "create another app as sample and I can deploy it to
confirm it is nothing with the infrastructure". The sample only proves
anything if it shares NOTHING with the engine except the platform and the
Streamlit runtime pin. An import of utils.styles, a copied theme file or a
config.py on its stage would make its result meaningless.
"""
import ast
import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
SAMPLE = os.path.join(ROOT, "sample_app")
DEPLOY = os.path.join(ROOT, "deploy.py")
PS1 = os.path.join(ROOT, "deploy_all.ps1")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _imports(path):
    names = set()
    for node in ast.walk(ast.parse(_source(path))):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add(node.module or "")
    return names


def test_the_sample_imports_only_streamlit():
    for fname in ("app.py", os.path.join("pages", "2_Second_Page.py")):
        assert _imports(os.path.join(SAMPLE, fname)) <= {"streamlit", "os"}, (
            fname + " imports something other than streamlit. The control "
            "app must share no code with the engine or its result proves "
            "nothing.")


def test_the_sample_pins_the_same_runtime_as_the_engine():
    """The runtime is the ONE thing deliberately shared."""
    engine = re.search(r"streamlit=([\d.]+)",
                       _source(os.path.join(ROOT, "streamlit_app", "environment.yml")))
    sample = re.search(r"streamlit=([\d.]+)",
                       _source(os.path.join(SAMPLE, "environment.yml")))
    assert engine and sample
    assert engine.group(1) == sample.group(1), (
        "The sample pins a different Streamlit version from the engine, so a "
        "difference between the two apps could be the version rather than "
        "the code.")


def test_the_sample_ships_the_engine_theme_unchanged():
    """Round 2 adds the theme. A diverged copy would prove nothing."""
    engine = _source(os.path.join(ROOT, "streamlit_app", ".streamlit", "config.toml"))
    sample = _source(os.path.join(SAMPLE, ".streamlit", "config.toml"))
    assert engine == sample, (
        "sample_app/.streamlit/config.toml differs from the engine's. The "
        "round-2 control must carry the engine's theme byte for byte.")
    assert not os.path.exists(os.path.join(SAMPLE, "config.py"))


def test_the_deploy_path_uploads_only_the_sample_files():
    src = _source(DEPLOY)
    body = src[src.index("def deploy_sample_app("):]
    body = body[:body.index("\ndef ", 10)]
    assert "'sample_app'" in body
    assert "config.py" not in body.split('"""')[2], (
        "deploy_sample_app uploads config.py — the control must not carry "
        "the engine's config.")
    assert "utils" not in body.split('"""')[2]
    assert "LINE_PROBE" in body


def test_the_flag_is_exclusive():
    """--sample-app must never also redeploy the engine."""
    src = _source(DEPLOY)
    assert "if args.sample_app:\n        deploy_db = deploy_st = deploy_nb = False" in src
    assert "ok = deploy_sample_app(session)" in src


def test_the_powershell_mode_reaches_the_flag():
    src = _source(PS1)
    assert '"sample"' in src
    assert '$deployArgs = @("--sample-app")' in src
    assert "-and -not $deploySample" in src, (
        "The 'Nothing to deploy' guard does not know about sample mode, so "
        "-Mode sample would exit before running deploy.py.")


_PROBE_CONSTANTS = ("ENGINE_THEME", "ENGINE_THEME_DOTTED", "PROBE_VARIANTS",
                    "RETIRED_PROBES", "PROBE_RUNTIME_THEME_APP")


def _constant(name):
    """A module-level constant from deploy.py, evaluated from source.

    deploy.py imports the repo config and the Snowflake libraries at import
    time, so only the three probe assignments are executed, in order (the
    variants are built from ENGINE_THEME by concatenation, so a plain
    literal_eval is not enough)."""
    ns = {}
    for node in ast.parse(_source(DEPLOY)).body:
        if isinstance(node, ast.Assign) and any(
                isinstance(tg, ast.Name) and tg.id in _PROBE_CONSTANTS
                for tg in node.targets):
            exec(compile(ast.Module([node], []), DEPLOY, "exec"), ns)
    assert name in ns, name + " is not a module-level constant in deploy.py"
    return ns[name]


def test_the_engine_theme_constant_matches_the_theme_file():
    """The fix-candidate variants must carry the REAL engine theme."""
    theme = _source(os.path.join(ROOT, "streamlit_app", ".streamlit", "config.toml"))
    engine_lines = [ln.strip() for ln in theme.splitlines()
                    if ln.strip() and not ln.strip().startswith("#")]
    const_lines = [ln.strip() for ln in _constant("ENGINE_THEME").splitlines()
                   if ln.strip()]
    assert const_lines == engine_lines, (
        "ENGINE_THEME in deploy.py has drifted from "
        "streamlit_app/.streamlit/config.toml.")


def test_the_variants_are_distinct_and_the_retired_ones_are_gone():
    live = [n for n, _ in _constant("PROBE_VARIANTS")]
    retired = _constant("RETIRED_PROBES")
    assert len(set(live)) == len(live)
    assert not set(live) & set(retired), (
        "A variant is both live and retired; the deploy would drop it and "
        "then recreate it.")
    assert all(n.startswith("LINE_PROBE_T") for n in live + retired)
    assert "LINE_PROBE" not in retired, "never retire the reference app"


def test_the_dotted_theme_is_the_same_config_without_the_header():
    """Round 5 rests on the two forms being identical to Streamlit."""
    import tomllib
    sectioned = tomllib.loads(_constant("ENGINE_THEME"))
    dotted = tomllib.loads(_constant("ENGINE_THEME_DOTTED"))
    assert dotted == sectioned, (
        "The dotted-key theme does not parse to the same table as the "
        "[theme] section, so a difference between T11 and LINE_PROBE could "
        "be a config difference rather than the header.")
    assert "[theme]" not in _constant("ENGINE_THEME_DOTTED")


def test_the_runtime_theme_matches_the_engine_theme_file():
    """Round 6 sets the theme from the script; the values must be the file's."""
    import tomllib
    src = _source(os.path.join(SAMPLE, "app.py"))
    tree = ast.parse(src)
    theme = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(tg, ast.Name) and tg.id == "_THEME" for tg in node.targets):
            theme = ast.literal_eval(node.value)
    assert theme, "_THEME is not a literal dict in sample_app/app.py"
    file_theme = tomllib.loads(
        _source(os.path.join(ROOT, "streamlit_app", ".streamlit", "config.toml")))["theme"]
    assert {k.split(".", 1)[1]: v for k, v in theme.items()} == file_theme, (
        "The runtime theme in sample_app/app.py differs from the engine's "
        "config.toml, so T12 would not be testing the engine's theme.")
    assert "st.rerun()" in src, (
        "The first run must rerun once, or the theme only shows after the "
        "user's first interaction.")
    assert 'get_option("theme.base") != "light"' in src, (
        "The rerun must be guarded, or every run reruns forever.")


def test_the_runtime_theme_app_ships_no_config_file():
    src = _source(DEPLOY)
    body = src[src.index("    # Round 6: no .streamlit/ at all"):]
    body = body[:body.index("_deploy_probe(") + 300]
    assert "runtime_theme.flag" in body
    assert ".toml" not in body, "T12 must not carry any config file"
