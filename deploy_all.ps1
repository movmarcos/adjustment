# Pull latest changes and deploy ONLY what changed.
#
# Usage:
#   .\deploy_all.ps1            # auto: deploy only the parts changed since last deploy
#   .\deploy_all.ps1 -Mode all         # force full deploy (DB + Streamlit)
#   .\deploy_all.ps1 -Mode db          # force DB objects only
#   .\deploy_all.ps1 -Mode streamlit   # force Streamlit app only
#
# How "auto" decides (the flag is computed from git, so it can never drift):
#   - a change under  new_adjustment_db_objects\  (or config.py / deploy.py) -> deploy DB
#   - a change under  streamlit_app\              (or config.py)             -> deploy Streamlit
#   config.py touches both. The last successfully-deployed commit is stored in
#   .last_deploy_commit (gitignored, local). On a failed deploy the marker is
#   NOT advanced, so the next run retries the same scope.

param(
    [ValidateSet("auto", "db", "streamlit", "all")]
    [string]$Mode = "auto"
)

$pythonExe   = "C:/Users/n319464/AppData/Local/Programs/Python/Python313/python.exe"
$deployScript = "deploy.py"
$markerFile  = ".last_deploy_commit"

# ── [1/3] Pull ───────────────────────────────────────────────────────────────
Write-Host "[1/3] Pulling latest changes from git..." -ForegroundColor Cyan
git pull
if ($LASTEXITCODE -ne 0) {
    Write-Host "Git pull failed. Aborting deployment." -ForegroundColor Red
    exit 1
}
$head = (git rev-parse HEAD).Trim()

# ── [2/3] Decide what to deploy ──────────────────────────────────────────────
$deployDb = $false
$deployStreamlit = $false

switch ($Mode) {
    "all"       { $deployDb = $true; $deployStreamlit = $true }
    "db"        { $deployDb = $true }
    "streamlit" { $deployStreamlit = $true }
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
            $deployDb = $true; $deployStreamlit = $true
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
                if ($f -like "new_adjustment_db_objects/*" -or $f -eq "config.py" -or $f -eq "deploy.py") {
                    $deployDb = $true
                }
                if ($f -like "streamlit_app/*" -or $f -eq "config.py") {
                    $deployStreamlit = $true
                }
            }
        }
    }
}

Write-Host ""
Write-Host ("Deploy plan  ->  DB objects: {0}   Streamlit app: {1}" -f `
            $(if ($deployDb) {"YES"} else {"skip"}),
            $(if ($deployStreamlit) {"YES"} else {"skip"})) -ForegroundColor Cyan

if (-not $deployDb -and -not $deployStreamlit) {
    Write-Host "Nothing to deploy for this change set." -ForegroundColor Green
    Set-Content -Path $markerFile -Value $head -NoNewline
    exit 0
}

# Map the flags to deploy.py arguments.
$deployArgs = @()
if ($deployDb -and -not $deployStreamlit)      { $deployArgs = @("--db-only") }
elseif ($deployStreamlit -and -not $deployDb)  { $deployArgs = @("--streamlit-only") }
# both -> no flag = full deploy

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
