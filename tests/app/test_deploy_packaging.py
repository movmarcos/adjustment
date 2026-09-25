"""The deployable package: streamlit/ must be copyable and self-contained.

Marcos, 2026-09-25: "I want to copy that streamlit folder and just have
everything there… if I run the script from the streamlit folder we don't do
a git pull and no connection to github. If I run from the root, it keeps the
same way."

So there are two entry points and they must not blur into each other:

    streamlit/deploy.ps1   hand-over. -DbTarget names the database, its first
                           four letters name the environment, no git at all.
    deploy_all.ps1 (root)  development. Pulls from git and deploys only what
                           changed.

And the target resolution has one property that is easy to lose and expensive
to lose: a deployed app must know which environment it was deployed INTO.
Streamlit in Snowflake sets no environment variables, so deploy.py bakes the
answer into a module it ships with the app. Without it an app deployed to
TEST reads the default and queries the DVLP database BY NAME — see
pages/10_Tasks_Cost.py, which builds fully-qualified names from
config.DATABASE.
"""
import importlib
import os
import re
import sys
import types

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PKG = os.path.join(ROOT, "streamlit")
APP = os.path.join(PKG, "adjustment_engine")

if PKG not in sys.path:
    sys.path.insert(0, PKG)

import config  # noqa: E402  (streamlit/config.py)


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ── The package is self-contained ────────────────────────────────────────────

def test_the_package_carries_everything_a_deploy_needs():
    for name in ("deploy.ps1", "deploy.py", "config.py"):
        assert os.path.isfile(os.path.join(PKG, name)), (
            name + " is not in streamlit/. Copying that folder has to give a "
            "complete, deployable solution.")
    for name in ("app.py", "app.toml", "environment.yml", "requirements.txt"):
        assert os.path.isfile(os.path.join(APP, name)), name + " missing from the app"
    for name in ("pages", "utils", "sql"):
        assert os.path.isdir(os.path.join(APP, name)), name + "/ missing from the app"
    assert len([f for f in os.listdir(os.path.join(APP, "sql")) if f.endswith(".sql")]) > 10


def test_the_app_folder_holds_no_development_kit():
    """Its unit tests live in tests/app/ so a copy carries the app alone."""
    assert not os.path.isdir(os.path.join(APP, "tests")), (
        "the app folder has a tests/ directory again — it would travel with "
        "every copy of the deployable package")


def test_the_deploy_script_reaches_outside_the_package_only_for_notebooks():
    """A '..' in deploy.py is a file the copied folder would not have."""
    src = _source(os.path.join(PKG, "deploy.py"))
    outward = [ln.strip() for ln in src.splitlines()
               if "Path(__file__).parent.parent" in ln]
    assert len(outward) == 1 and "notebooks" in outward[0], (
        "deploy.py reads something outside streamlit/: " + repr(outward)
        + ". Only the optional notebooks kit may live outside the package.")


def test_missing_notebooks_are_not_fatal_to_a_copied_package():
    src = _source(os.path.join(PKG, "deploy.py"))
    assert "if not nb_dir.is_dir():" in src


# ── The database names the environment ───────────────────────────────────────

@pytest.mark.parametrize("db,env", [
    ("DVLP_RAPTOR_NEWADJ_4", "DVLP"),
    ("TEST_RAPTOR_NEWADJ_4", "TEST"),
    ("RLSE_RAPTOR_NEWADJ_4", "RLSE"),
    ("PROD_RAPTOR_NEWADJ_4", "PROD"),
    ("prod_raptor_newadj_4", "PROD"),
    ("  TEST_ANYTHING_AT_ALL  ", "TEST"),
])
def test_the_first_four_letters_are_the_environment(db, env):
    assert config.env_from_database(db) == env


@pytest.mark.parametrize("bad", ["WRONG_RAPTOR_NEWADJ_4", "", "DV_RAPTOR", "RAPTOR_PROD"])
def test_a_database_that_names_no_environment_is_refused(bad):
    """Refused before anything connects: a typo must not create objects."""
    with pytest.raises(ValueError) as e:
        config.env_from_database(bad)
    assert "first four letters" in str(e.value)


# ── Resolution order ─────────────────────────────────────────────────────────

@pytest.fixture
def resolved(monkeypatch):
    """Reload config under a given environment, then restore the default."""
    def _load(env_vars=None, deploy_target=None):
        for var in ("ADJ_DB", "ADJ_ENV"):
            monkeypatch.delenv(var, raising=False)
        for k, v in (env_vars or {}).items():
            monkeypatch.setenv(k, v)
        if deploy_target is None:
            monkeypatch.delitem(sys.modules, "deploy_target", raising=False)
            # ...and make a real import fail, so the file cannot leak in
            monkeypatch.setitem(sys.modules, "deploy_target", None)
        else:
            mod = types.ModuleType("deploy_target")
            mod.DATABASE = deploy_target
            monkeypatch.setitem(sys.modules, "deploy_target", mod)
        return importlib.reload(config)
    yield _load
    for var in ("ADJ_DB", "ADJ_ENV"):
        os.environ.pop(var, None)
    sys.modules.pop("deploy_target", None)
    importlib.reload(config)


def test_the_default_is_the_development_environment(resolved):
    c = resolved()
    assert (c.ENV, c.DATABASE) == ("DVLP", "DVLP_RAPTOR_NEWADJ_4")


def test_adj_db_names_the_database_verbatim(resolved):
    c = resolved({"ADJ_DB": "TEST_RAPTOR_NEWADJ_4"})
    assert c.ENV == "TEST"
    assert c.DATABASE == "TEST_RAPTOR_NEWADJ_4"
    assert c.WAREHOUSE == "TEST_RAPTOR_WH"
    assert c.ROLE_OWNER == "TEST_RAPTOR_OWNER"
    assert c.DEPLOY_USER.startswith("apt_")


def test_a_database_outside_the_naming_pattern_is_still_used_verbatim(resolved):
    """-DbTarget is a database name, not a pattern to re-derive."""
    c = resolved({"ADJ_DB": "PROD_RAPTOR_SOMETHING_ELSE"})
    assert c.DATABASE == "PROD_RAPTOR_SOMETHING_ELSE" and c.ENV == "PROD"


def test_adj_db_wins_over_adj_env(resolved):
    c = resolved({"ADJ_DB": "PROD_RAPTOR_NEWADJ_4", "ADJ_ENV": "DVLP"})
    assert c.ENV == "PROD"


def test_the_baked_deploy_target_wins_over_everything(resolved):
    """This is the deployed app's only way of knowing where it lives."""
    c = resolved({"ADJ_DB": "DVLP_RAPTOR_NEWADJ_4"}, deploy_target="RLSE_RAPTOR_NEWADJ_4")
    assert (c.ENV, c.DATABASE) == ("RLSE", "RLSE_RAPTOR_NEWADJ_4")


def test_adj_env_still_works_on_its_own(resolved):
    c = resolved({"ADJ_ENV": "RLSE"})
    assert (c.ENV, c.DATABASE) == ("RLSE", "RLSE_RAPTOR_NEWADJ_4")


def test_a_bad_adj_env_is_refused(resolved):
    with pytest.raises(ValueError):
        resolved({"ADJ_ENV": "NOPE"})


# ── deploy.py: the app manifest and what reaches the stage ───────────────────

@pytest.fixture(scope="module")
def deploy_mod():
    sys.path.insert(0, PKG)
    import deploy
    return deploy


def test_one_folder_beside_the_script_is_one_app(deploy_mod):
    apps = deploy_mod.streamlit_apps()
    assert [d.name for d in apps] == ["adjustment_engine"]


def test_the_manifest_names_and_titles_the_app(deploy_mod):
    m = deploy_mod.load_app_manifest(deploy_mod.streamlit_apps()[0])
    assert m["name"] == "ADJUSTMENT_ENGINE", (
        "the Snowflake object name must stay stable — changing it creates a "
        "second app at a new URL and orphans the first")
    assert m["title"] == "MRM Adjustment Engine"
    assert m["stage"] == "STREAMLIT_ADJUSTMENT_STAGE", (
        "the stage name is deliberately NOT derived from the app name: this "
        "stage already exists in every environment")
    assert os.path.isfile(os.path.join(APP, m["main_file"]))


def test_a_folder_with_no_manifest_still_deploys(deploy_mod, tmp_path):
    """Defaults come from the folder name, so app.toml is optional."""
    d = tmp_path / "my_new_app"
    d.mkdir()
    (d / "app.py").write_text("import streamlit")
    m = deploy_mod.load_app_manifest(d)
    assert m["name"] == "MY_NEW_APP" and m["title"] == "My New App"
    assert m["stage"] == "STREAMLIT_MY_NEW_APP_STAGE"


def test_the_created_app_carries_the_title_with_a_fallback(deploy_mod):
    src = _source(os.path.join(PKG, "deploy.py"))
    assert "TITLE           = '{_title}'" in src, (
        "Snowsight lists the app under TITLE; without it users see "
        "ADJUSTMENT_ENGINE")
    assert "retrying without it" in src, (
        "TITLE needs a recent Snowflake — a display name must not be able to "
        "fail the whole deploy")


def test_the_deploy_bakes_the_target_into_the_uploaded_app(deploy_mod, tmp_path):
    path = deploy_mod._write_deploy_target(tmp_path)
    text = path.read_text(encoding="utf-8")
    assert path.name == "deploy_target.py"
    assert f'DATABASE = "{config.DATABASE}"' in text
    assert f'ENV = "{config.ENV}"' in text
    src = _source(os.path.join(PKG, "deploy.py"))
    assert "_write_deploy_target(_target_dir)" in src, (
        "the baked target is generated but never uploaded — a deployed app "
        "would read config.py's default and query the wrong database")
    assert "tempfile.mkdtemp" in src, (
        "it must be written to a temp dir; a copy at the package root would "
        "pin every later deploy to one environment")


def test_the_stage_gets_the_app_and_not_the_deploy_inputs():
    """sql/ and app.toml are deploy INPUTS: they must never reach the stage.

    Docstring and comments are stripped first — both explain the exclusion,
    so a plain substring search would find the explanation and pass whether
    or not the code still honoured it.
    """
    src = _source(os.path.join(PKG, "deploy.py"))
    body = src[src.index("def deploy_streamlit_app("):src.index("# ─── Deploy Notebooks")]
    body = re.sub(r'"""(?:.|\n)*?"""', "", body, count=1)          # docstring
    code = "\n".join(ln for ln in body.splitlines()
                     if not ln.strip().startswith("#"))             # comments
    assert "app['main_file']" in code
    assert "'sql'" not in code, "sql/ is a deploy input, not part of the running app"
    assert "app.toml" not in code, "the manifest does not belong on the stage"


def test_the_db_objects_come_from_the_app_folder():
    src = _source(os.path.join(PKG, "deploy.py"))
    assert "def deploy_db_objects(session, app_dir):" in src
    assert "Path(app_dir) / 'sql'" in src


# ── The two entry points stay distinct ───────────────────────────────────────

GIT_VERBS = ("git fetch", "git pull", "git checkout", "git rev-parse", "git diff")


def test_the_handover_script_never_touches_git():
    ps1 = _source(os.path.join(PKG, "deploy.ps1"))
    for verb in GIT_VERBS:
        assert verb not in ps1, (
            "streamlit/deploy.ps1 runs '" + verb + "'. Run from a copied "
            "folder there is no repository and no network to GitHub.")
    assert "last_deploy_commit" not in ps1


def test_the_handover_script_requires_a_database_and_validates_it():
    ps1 = _source(os.path.join(PKG, "deploy.ps1"))
    assert "[Parameter(Mandatory = $true)]" in ps1 and "$DbTarget" in ps1, (
        "a hand-over script must never guess an environment")
    assert '$validEnvs = @("DVLP", "TEST", "RLSE", "PROD")' in ps1
    assert "$env:ADJ_DB = $db" in ps1
    assert 'Substring(0, [Math]::Min(4, $db.Length))' in ps1


def test_the_handover_script_asks_before_the_environments_that_matter():
    ps1 = _source(os.path.join(PKG, "deploy.ps1"))
    assert '$env4 -eq "PROD" -or $env4 -eq "RLSE"' in ps1
    assert "Read-Host" in ps1 and "-not $Yes" in ps1


def test_the_handover_script_runs_the_engine_beside_it():
    ps1 = _source(os.path.join(PKG, "deploy.ps1"))
    assert 'Join-Path $PSScriptRoot "deploy.py"' in ps1, (
        "it must run the deploy.py next to it, whatever the caller's cwd")


def test_the_development_script_still_pulls_from_git():
    ps1 = _source(os.path.join(ROOT, "deploy_all.ps1"))
    assert "git pull" in ps1 and "git fetch" in ps1
    assert '$deployScript = "streamlit/deploy.py"' in ps1
    assert "streamlit/*/sql/*" in ps1, (
        "the change detector still looks for the old folder names")
