# Superset Dashboard Register — launcher
# Uses the repo's existing .venv (which already has fastapi/httpx/openpyxl).
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPy = Join-Path (Split-Path -Parent $here) ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) { $venvPy = "python" }
Push-Location $here
try {
  Write-Host "Starting Superset Dashboard Register on http://127.0.0.1:8099 ..." -ForegroundColor Cyan
  & $venvPy -m uvicorn app:app --host 127.0.0.1 --port 8099
} finally {
  Pop-Location
}
