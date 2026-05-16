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
$env:LHS_BIND = if ($env:LHS_BIND) { $env:LHS_BIND } else { $env:LHS_HOST }
$env:LHS_PORT = if ($env:LHS_PORT) { $env:LHS_PORT } else { "7878" }

if ($env:LHS_BIND -ne "127.0.0.1" -and $env:LHS_BIND -ne "localhost" -and -not $env:LHS_AUTH_TOKEN) {
  Write-Host "WARN: binding to non-loopback ($($env:LHS_BIND)) without LHS_AUTH_TOKEN. Anyone on the network can install/clean stacks." -ForegroundColor Yellow
}

Write-Host "LakeHouse Studio listening on $($env:LHS_BIND):$($env:LHS_PORT) (open http://$($env:LHS_HOST):$($env:LHS_PORT))" -ForegroundColor Green
& $py -m uvicorn backend.main:app --host $env:LHS_BIND --port ([int]$env:LHS_PORT)
