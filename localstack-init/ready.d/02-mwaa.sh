#!/bin/bash
# LocalStack MWAA requires Ultimate/Enterprise (not Hobby/Base). Set ENABLE_LOCALSTACK_MWAA=1 and a valid token.
set -u
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

if [ "${ENABLE_LOCALSTACK_MWAA:-0}" != "1" ]; then
  echo "[mwaa] Skipping MWAA (set ENABLE_LOCALSTACK_MWAA=1 to attempt). On Hobby/Base plans use: docker compose --profile airflow up -d"
  exit 0
fi

if [ -z "${LOCALSTACK_AUTH_TOKEN:-}" ]; then
  echo "[mwaa] LOCALSTACK_AUTH_TOKEN empty; skipping MWAA."
  exit 0
fi

NAME="${MWAA_ENV_NAME:-local-mwaa-env}"

if awslocal mwaa get-environment --name "$NAME" >/dev/null 2>&1; then
  echo "[mwaa] Environment $NAME already exists."
  exit 0
fi

echo "[mwaa] Creating MWAA environment $NAME (requires Ultimate/Enterprise)..."
if awslocal mwaa create-environment \
  --name "$NAME" \
  --dag-s3-path /dags \
  --execution-role-arn arn:aws:iam::000000000000:role/airflow-role \
  --network-configuration "{}" \
  --source-bucket-arn arn:aws:s3:::mwaa-dags-bucket \
  --airflow-version 2.10.3; then
  echo "[mwaa] create-environment submitted. Use: awslocal mwaa get-environment --name $NAME"
else
  echo "[mwaa] create-environment failed (check plan includes MWAA). Use standalone Airflow profile instead."
fi
exit 0
