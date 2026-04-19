# Requires: awslocal (pip install awscli-local) and Docker running.
$ErrorActionPreference = "Stop"
$endpoint = "http://127.0.0.1:4566"
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"

Write-Host "Checking LocalStack at $endpoint ..."
try {
    Invoke-RestMethod -Uri "$endpoint/_localstack/health" -Method Get | ConvertTo-Json -Depth 5
} catch {
    Write-Error "LocalStack health check failed. Start: docker compose up -d"
    throw
}

if (-not (Get-Command awslocal -ErrorAction SilentlyContinue)) {
    Write-Warning "awslocal not found. Install: pip install awscli-local"
    exit 1
}

Write-Host "`nRedshift cluster:"
awslocal redshift describe-clusters --cluster-identifier local-redshift --output table

Write-Host "`nS3 bucket (DAGs):"
awslocal s3 ls s3://mwaa-dags-bucket/dags/ 2>$null

Write-Host "`nOptional MWAA:"
awslocal mwaa list-environments 2>$null

Write-Host "`nDone."
