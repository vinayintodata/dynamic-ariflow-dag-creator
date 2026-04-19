#Requires -Version 5.1
<#
  Starts LocalStack (docker_mwaa), waits for health, applies LocalStack patches to aws-mwaa-local-runner,
  builds amazon/mwaa-local:2_10_3, syncs DAGs, and starts MWAA local-runner on http://127.0.0.1:8080 (admin / test).

  Run from repo root:  .\scripts\setup-all.ps1
#>
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$mwaa = Join-Path $root "aws-mwaa-local-runner"
$patches = Join-Path $root "patches\aws-mwaa-local-runner"
$composeProject = "aws-mwaa-local-runner-2_10_3"

function Copy-MwaaPatches {
    if (-not (Test-Path $patches)) {
        throw "Patch directory missing: $patches"
    }
    Write-Host "Applying patches from $patches -> $mwaa" -ForegroundColor DarkGray
    $dstDocker = Join-Path $mwaa "docker"
    Copy-Item -Force (Join-Path $patches "docker\docker-compose-local.yml") (Join-Path $dstDocker "docker-compose-local.yml")
    Copy-Item -Force (Join-Path $patches "docker\config\.env.localrunner") (Join-Path $dstDocker "config\.env.localrunner")
}

function Sync-DagsToMwaa {
    $src = Join-Path $root "dags"
    $dst = Join-Path $mwaa "dags"
    if (-not (Test-Path $src)) { return }
    Get-ChildItem $src -Filter "*.py" | ForEach-Object {
        Copy-Item -Force $_.FullName (Join-Path $dst $_.Name)
        Write-Host "Synced DAG: $($_.Name)" -ForegroundColor DarkGray
    }
}

if (-not (Test-Path $mwaa)) {
    Write-Host "Cloning aws-mwaa-local-runner (v2.10.3)..." -ForegroundColor Cyan
    git clone --depth 1 --branch v2.10.3 https://github.com/aws/aws-mwaa-local-runner.git $mwaa
}

Copy-MwaaPatches
Sync-DagsToMwaa

Write-Host "=== [1/4] Starting LocalStack ===" -ForegroundColor Cyan
Set-Location $root
docker compose up -d

$healthUrl = "http://127.0.0.1:4566/_localstack/health"
$deadline = (Get-Date).AddMinutes(6)
$poll = 0
do {
    try {
        Invoke-RestMethod -Uri $healthUrl -Method Get -TimeoutSec 5 | Out-Null
        Write-Host "LocalStack is healthy." -ForegroundColor Green
        break
    } catch {
        $poll++
        if (($poll % 5) -eq 0) {
            Write-Host "Still waiting for LocalStack ($healthUrl)..." -ForegroundColor DarkYellow
        }
        Start-Sleep -Seconds 3
    }
} while ((Get-Date) -lt $deadline)
if ((Get-Date) -ge $deadline) {
    throw "LocalStack did not become healthy in time. Check: docker compose logs localstack"
}

Write-Host "=== [2/4] Building MWAA local image (amazon/mwaa-local:2_10_3) - this can take several minutes ===" -ForegroundColor Cyan
Set-Location $mwaa
docker build --rm --compress -t amazon/mwaa-local:2_10_3 ./docker

Write-Host "=== [3/4] Starting MWAA local-runner (detached) ===" -ForegroundColor Cyan
docker compose -p $composeProject --file docker/docker-compose-local.yml up -d

Write-Host "=== [4/4] Status ===" -ForegroundColor Cyan
docker compose -p $composeProject --file docker/docker-compose-local.yml ps

Write-Host ""
Write-Host "Airflow UI:  http://127.0.0.1:8080  (user: admin, password: test)" -ForegroundColor Green
Write-Host "LocalStack:  http://127.0.0.1:4566" -ForegroundColor Green
Write-Host "Logs:        docker compose -p $composeProject --file docker/docker-compose-local.yml logs --follow local-runner" -ForegroundColor Gray
