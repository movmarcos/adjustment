# DEVELOPMENT deploy: pull the latest changes and deploy ONLY what changed.
#
# For a hand-over or a named environment use deploy.ps1 instead — it takes
# -DbTarget, touches no git and keeps no marker file.
#
# Usage:
#   .\deploy_all.ps1            # auto: deploy only the parts changed since last deploy
#   .\deploy_all.ps1 -Mode all         # force full deploy (DB + Streamlit + Notebooks)
#   .\deploy_all.ps1 -Mode db          # force DB objects only
#   .\deploy_all.ps1 -Mode streamlit   # force Streamlit app only
#   .\deploy_all.ps1 -Mode notebooks   # force Snowflake Notebooks only
#   .\deploy_all.ps1 -Branch my-branch # deploy from a feature branch instead of main
#   .\deploy_all.ps1 -DbTarget TEST_RAPTOR_NEWADJ_4     # target another database
#   .\deploy_all.ps1 -PythonExe C:/path/to/python.exe   # override the interpreter
#
# How "auto" decides (the flag is computed from git, so it can never drift):
#   - a change under  streamlit\<app>\sql\   (or streamlit\config.py|deploy.py) -> DB
#   - a change elsewhere under streamlit\   (or streamlit\config.py)          -> Streamlit
#   - a change under  notebooks\             (or streamlit\deploy.py)          -> Notebooks
#   config.py touches DB and Streamlit. The last successfully-deployed commit is stored
#   in .last_deploy_commit (gitignored, local). On a failed deploy the marker is
#   NOT advanced, so the next run retries the same scope.

param(
    [ValidateSet("auto", "db", "streamlit", "notebooks", "all")]
    [string]$Mode = "auto",
    # Git branch to deploy from. Default: main.
    # Point it at a feature branch with -Branch while testing one.
    [string]$Branch = "main",
    # Optional: deploy to a named database instead of config.py's default.
    # The first four letters are the environment (see deploy.ps1).
    [string]$DbTarget = "",
    # Python interpreter to run deploy.py with. Defaults to "python" resolved
    # via PATH, or $env:ADJ_DEPLOY_PYTHON when set — never hardcode a
    # personal machine path here (config.py's whole premise is nothing else
    # in the repo hardcodes an environment/machine detail).
    [string]$PythonExe = $(if ($env:ADJ_DEPLOY_PYTHON) { $env:ADJ_DEPLOY_PYTHON } else { "python" })
)

if ($DbTarget) {
    $db = $DbTarget.Trim().ToUpper()
    $env4 = $db.Substring(0, [Math]::Min(4, $db.Length))
    if (@("DVLP","TEST","RLSE","PROD") -notcontains $env4) {
        Write-Host "Database '$db' does not name an environment (first four letters: '$env4')." -ForegroundColor Red
        exit 1
    }
    $env:ADJ_DB = $db
    Write-Host "Target database: $db  (environment $env4)" -ForegroundColor Cyan
}

$pythonExe   = $PythonExe
$deployScript = "streamlit/deploy.py"
$markerFile  = ".last_deploy_commit"

# ── [1/3] Pull ───────────────────────────────────────────────────────────────
Write-Host "[1/3] Fetching and switching to branch '$Branch'..." -ForegroundColor Cyan
git fetch origin
if ($LASTEXITCODE -ne 0) {
    Write-Host "Git fetch failed. Aborting deployment." -ForegroundColor Red
    exit 1
}
$current = (git rev-parse --abbrev-ref HEAD).Trim()
if ($current -ne $Branch) {
    git checkout $Branch
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Could not check out branch '$Branch'. Aborting deployment." -ForegroundColor Red
        exit 1
    }
}
git pull origin $Branch
if ($LASTEXITCODE -ne 0) {
    Write-Host "Git pull failed. Aborting deployment." -ForegroundColor Red
    exit 1
}
$head = (git rev-parse HEAD).Trim()

# ── [2/3] Decide what to deploy ──────────────────────────────────────────────
$deployDb = $false
$deployStreamlit = $false
$deployNotebooks = $false

switch ($Mode) {
    "all"       { $deployDb = $true; $deployStreamlit = $true; $deployNotebooks = $true }
    "db"        { $deployDb = $true }
    "streamlit" { $deployStreamlit = $true }
    "notebooks" { $deployNotebooks = $true }
    default {
        # auto — diff against the last deployed commit
        $baseline = $null
        if (Test-Path $markerFile) { $baseline = (Get-Content $markerFile -Raw).Trim() }

        $baselineValid = $false
        if ($baseline) {
            git cat-file -e "$baseline^{commit}" 2>$null
            if ($LASTEXITCODE -eq 0) { $baselineValid = $true }
        }

        if (-not $baselineValid) {
            Write-Host "No valid deploy marker - deploying everything (first run)." -ForegroundColor Yellow
            $deployDb = $true; $deployStreamlit = $true; $deployNotebooks = $true
        }
        else {
            $changed = git diff --name-only "$baseline" "$head"
            if (-not $changed) {
                Write-Host "No changes since last deploy ($baseline). Nothing to do." -ForegroundColor Green
                Set-Content -Path $markerFile -Value $head -NoNewline
                exit 0
            }
            Write-Host "Changed files since last deploy:" -ForegroundColor Cyan
            $changed | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
            foreach ($f in $changed) {
                if ($f -like "streamlit/*/sql/*" -or $f -eq "streamlit/config.py" -or $f -eq "streamlit/deploy.py") {
                    $deployDb = $true
                }
                if (($f -like "streamlit/*" -and $f -notlike "streamlit/*/sql/*") -or $f -eq "streamlit/config.py") {
                    $deployStreamlit = $true
                }
                if ($f -like "notebooks/*" -or $f -eq "streamlit/deploy.py") {
                    $deployNotebooks = $true
                }
            }
        }
    }
}

Write-Host ""
Write-Host ("Deploy plan  ->  DB objects: {0}   Streamlit app: {1}   Notebooks: {2}" -f `
            $(if ($deployDb) {"YES"} else {"skip"}),
            $(if ($deployStreamlit) {"YES"} else {"skip"}),
            $(if ($deployNotebooks) {"YES"} else {"skip"})) -ForegroundColor Cyan

if (-not $deployDb -and -not $deployStreamlit -and -not $deployNotebooks) {
    Write-Host "Nothing to deploy for this change set." -ForegroundColor Green
    Set-Content -Path $markerFile -Value $head -NoNewline
    exit 0
}

# Map the flags to deploy.py arguments.
$deployArgs = @()
if ($deployDb -and -not $deployStreamlit -and -not $deployNotebooks)        { $deployArgs = @("--db-only") }
elseif ($deployStreamlit -and -not $deployDb -and -not $deployNotebooks)    { $deployArgs = @("--streamlit-only") }
elseif ($deployNotebooks -and -not $deployDb -and -not $deployStreamlit)    { $deployArgs = @("--notebooks-only") }
# any other combination (incl. all three) -> no flag = full deploy.py run
#
# NB: full deploy also runs the parts NOT flagged here (e.g. two-of-three
# changed -> deploy.py runs DB + Streamlit + Notebooks, a superset of what
# changed). That is intentional and matches this script's existing
# behaviour for DB+Streamlit before Notebooks was added.

# ── [3/3] Deploy ─────────────────────────────────────────────────────────────
Write-Host "[3/3] Running: python $deployScript $deployArgs" -ForegroundColor Cyan
& $pythonExe $deployScript @deployArgs

if ($LASTEXITCODE -eq 0) {
    Set-Content -Path $markerFile -Value $head -NoNewline
    Write-Host "Deployment completed successfully! (marker -> $head)" -ForegroundColor Green
} else {
    Write-Host "Deployment failed - marker NOT advanced, same scope retries next run." -ForegroundColor Red
    exit 1
}
