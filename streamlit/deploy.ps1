# Deploy the Adjustment Engine to a named Snowflake database.
#
# It lives in this folder on purpose: streamlit\ holds the deploy engine
# (deploy.py), the environment map (config.py) and every app folder, so
# COPYING THIS FOLDER GIVES A COMPLETE, DEPLOYABLE SOLUTION. Run it from
# here and nothing touches git: no fetch, no branch, no marker file.
# (..\deploy_all.ps1 is the development twin, and it still pulls from git
# and deploys only what changed.)
#
# Usage:
#   cd streamlit
#   .\deploy.ps1 -DbTarget TEST_RAPTOR_NEWADJ_4
#   .\deploy.ps1 -DbTarget PROD_RAPTOR_NEWADJ_4 -Mode streamlit
#   .\deploy.ps1 -DbTarget DVLP_RAPTOR_NEWADJ_4 -Mode db -Yes
#
# THE FIRST FOUR LETTERS OF -DbTarget ARE THE ENVIRONMENT: DVLP, TEST, RLSE
# or PROD. Everything else follows from it — warehouse ({ENV}_RAPTOR_WH),
# owner and read-only roles, and the deploy service account. A name whose
# first four letters are none of those is refused before anything connects.
#
# What gets deployed (one folder beside this script = one app):
#   db         the app's sql\*.sql in filename order, then task resume +
#              schema validation
#   streamlit  the app itself, uploaded to its stage and (re)created
#   notebooks  the Snowflake test-harness notebook
#   all        all three (default)

param(
    # Full database name, e.g. TEST_RAPTOR_NEWADJ_4. Required — there is no
    # default on purpose: a hand-over script must never guess an environment.
    [Parameter(Mandatory = $true)]
    [string]$DbTarget,

    [ValidateSet("all", "db", "streamlit", "notebooks")]
    [string]$Mode = "all",

    # Python interpreter. Defaults to "python" on PATH, or $env:ADJ_DEPLOY_PYTHON.
    [string]$PythonExe = $(if ($env:ADJ_DEPLOY_PYTHON) { $env:ADJ_DEPLOY_PYTHON } else { "python" }),

    # Skip the typed confirmation required for RLSE and PROD.
    [switch]$Yes
)

$ErrorActionPreference = "Stop"

# ── [1/3] Resolve the target ────────────────────────────────────────────────
$db  = $DbTarget.Trim().ToUpper()
$env4 = $db.Substring(0, [Math]::Min(4, $db.Length))
$validEnvs = @("DVLP", "TEST", "RLSE", "PROD")

if ($validEnvs -notcontains $env4) {
    Write-Host "Database '$db' does not name an environment." -ForegroundColor Red
    Write-Host "Its first four letters are '$env4'; expected one of: $($validEnvs -join ', ')." -ForegroundColor Red
    exit 1
}

# config.py resolves the target from ADJ_DB (it wins over ADJ_ENV), and
# deploy.py bakes the resolved values into the copy of config it uploads, so
# the deployed app knows which environment it is running in.
$env:ADJ_DB = $db

Write-Host ""
Write-Host "  Target database : $db"        -ForegroundColor Cyan
Write-Host "  Environment     : $env4  (first four letters)" -ForegroundColor Cyan
Write-Host "  Warehouse       : ${env4}_RAPTOR_WH" -ForegroundColor Cyan
Write-Host "  Owner role      : ${env4}_RAPTOR_OWNER" -ForegroundColor Cyan
Write-Host "  Deploying       : $Mode"      -ForegroundColor Cyan
Write-Host ""

# ── [2/3] Confirm the environments that matter ──────────────────────────────
if (($env4 -eq "PROD" -or $env4 -eq "RLSE") -and -not $Yes) {
    Write-Host "This deploys to $env4. Type the database name to continue:" -ForegroundColor Yellow
    $typed = Read-Host "  database"
    if ($typed.Trim().ToUpper() -ne $db) {
        Write-Host "Name did not match - nothing was deployed." -ForegroundColor Red
        exit 1
    }
}

# ── [3/3] Deploy ────────────────────────────────────────────────────────────
$deployArgs = @()
switch ($Mode) {
    "db"        { $deployArgs = @("--db-only") }
    "streamlit" { $deployArgs = @("--streamlit-only") }
    "notebooks" { $deployArgs = @("--notebooks-only") }
    # "all" -> no flag: deploy.py runs every phase
}

Write-Host "Running: $PythonExe deploy.py $deployArgs" -ForegroundColor Cyan
& $PythonExe (Join-Path $PSScriptRoot "deploy.py") @deployArgs
$code = $LASTEXITCODE

if ($code -eq 0) {
    Write-Host "Deployment to $db completed successfully." -ForegroundColor Green
} else {
    Write-Host "Deployment to $db FAILED (exit $code)." -ForegroundColor Red
}
exit $code
