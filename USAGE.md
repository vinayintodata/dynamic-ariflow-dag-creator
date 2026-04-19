# Local MWAA + LocalStack quick usage

This repo runs two stacks:
- LocalStack (S3, IAM, Redshift) via `docker-compose.yml`
- AWS MWAA local runner (Airflow) via `aws-mwaa-local-runner/docker-compose-local.yml`

## Prereqs
- Docker Desktop running
- PowerShell
- Python/pip (for awslocal/awscli)

## One-time setup
```powershell
cd <project-root>
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
python -m pip install awscli-local awscli
```

## Start LocalStack (Redshift, S3)
```powershell
docker compose up -d
.\scripts\verify-localstack.ps1   # shows health, Redshift, S3 seed
```
Local endpoints (from host): `http://127.0.0.1:4566` (AWS gateway).

## Start MWAA-like Airflow (local runner)
```powershell
cd aws-mwaa-local-runner
docker compose -p aws-mwaa-local-runner-2_10_3 --file docker/docker-compose-local.yml up -d
```
Notes:
- If port 8080 is busy, stop other containers using it.
- The `local-runner` container must be on both networks: `awslocal` and `aws-mwaa-local-runner-2_10_3_default`. If it ever gets stuck “waiting for Postgres,” run:
  ```powershell
  docker network connect aws-mwaa-local-runner-2_10_3_default aws-mwaa-local-runner-2_10_3-local-runner-1
  docker restart aws-mwaa-local-runner-2_10_3-local-runner-1
  ```
- Airflow UI: http://127.0.0.1:8080 (admin / test)
- LocalStack from containers: http://localstack:4566 (already wired in compose)

## Run / verify the Redshift DAG
Airflow CLI sometimes defaults to sqlite; override to Postgres for commands:
```powershell
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
$env:AIRFLOW_HOME="/usr/local/airflow"

docker compose -p aws-mwaa-local-runner-2_10_3 --file docker/docker-compose-local.yml exec local-runner `
  airflow dags list | Select-String redshift_localstack_smoke

docker compose -p aws-mwaa-local-runner-2_10_3 --file docker/docker-compose-local.yml exec local-runner `
  airflow tasks test redshift_localstack_smoke describe_redshift 2026-01-01
```
Expected: prints LocalStack Redshift endpoint and succeeds.

## Endpoint cheat sheet
- From host: `http://127.0.0.1:4566`
- From containers: `http://localstack:4566`
- AWS creds for LocalStack: `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, region `us-east-1`

## Optional: LocalStack MWAA API
Requires a LocalStack license. Set in `.env` then recreate LocalStack:
```
LOCALSTACK_AUTH_TOKEN=<token>
ENABLE_LOCALSTACK_MWAA=1
```

