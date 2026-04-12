# Pull latest changes from git and deploy Streamlit app and database objects
# Usage: .\deploy_all.ps1

Write-Host "[1/2] Pulling latest changes from git..." -ForegroundColor Cyan

git pull

if ($LASTEXITCODE -ne 0) {
    Write-Host "Git pull failed. Aborting deployment." -ForegroundColor Red
    exit 1
}

Write-Host "[2/2] Deploying Streamlit app and database objects..." -ForegroundColor Cyan

$pythonExe = "C:/Users/n319464/AppData/Local/Programs/Python/Python313/python.exe"
$deployScript = "deploy.py"

& $pythonExe $deployScript

if ($LASTEXITCODE -eq 0) {
    Write-Host "Deployment completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Deployment failed. Check the output above for errors." -ForegroundColor Red
    exit 1
}
