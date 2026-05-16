$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path "$root\.venv")) {
  Write-Host "Creating virtual environment..." -ForegroundColor Cyan
  python -m venv .venv
}

$pip = "$root\.venv\Scripts\pip.exe"
$py  = "$root\.venv\Scripts\python.exe"

& $pip install --quiet --disable-pip-version-check -r requirements.txt

$env:LHS_HOST = if ($env:LHS_HOST) { $env:LHS_HOST } else { "127.0.0.1" }
$env:LHS_PORT = if ($env:LHS_PORT) { $env:LHS_PORT } else { "7878" }

Write-Host "LakeHouse Studio starting at http://$($env:LHS_HOST):$($env:LHS_PORT)" -ForegroundColor Green
& $py -m uvicorn backend.main:app --host $env:LHS_HOST --port ([int]$env:LHS_PORT)
