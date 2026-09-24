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
        assert _imports(os.path.join(SAMPLE, fname)) == {"streamlit"}, (
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


def test_the_round_three_variants_cover_every_theme_setting_exactly_once():
    """A setting left out of the split could be the one that draws the line."""
    # Read the constant from source: deploy.py imports the repo's config and
    # snowflake libraries at module level, which a test must not need.
    tree = ast.parse(_source(DEPLOY))
    variants = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(tg, ast.Name) and tg.id == "PROBE_THEME_VARIANTS"
                for tg in node.targets):
            variants = ast.literal_eval(node.value)
    assert variants, "PROBE_THEME_VARIANTS is not a literal constant in deploy.py"

    class mod:  # keeps the assertions below unchanged
        PROBE_THEME_VARIANTS = variants

    theme = _source(os.path.join(ROOT, "streamlit_app", ".streamlit", "config.toml"))
    engine_lines = sorted(
        ln.strip() for ln in theme.splitlines()
        if "=" in ln and not ln.strip().startswith("#") and not ln.strip().startswith("["))
    variant_lines = sorted(line for _, line in mod.PROBE_THEME_VARIANTS)
    assert variant_lines == engine_lines, (
        "PROBE_THEME_VARIANTS does not match the engine theme line for line. "
        "Engine: " + repr(engine_lines) + " variants: " + repr(variant_lines))
    names = [n for n, _ in mod.PROBE_THEME_VARIANTS]
    assert len(set(names)) == len(names)
    assert all(n.startswith("LINE_PROBE_T") for n in names)
