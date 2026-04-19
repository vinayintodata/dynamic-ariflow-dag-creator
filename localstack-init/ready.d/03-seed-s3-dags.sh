#!/bin/bash
# Upload DAGs and optional requirements to the MWAA source bucket (synced by MWAA when enabled).
set -u
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

if [ "${SEED_MWAA_S3:-1}" != "1" ]; then
  echo "[seed] SEED_MWAA_S3=0; skipping S3 upload."
  exit 0
fi

if [ -d /opt/dags ] && [ "$(ls -A /opt/dags 2>/dev/null)" ]; then
  echo "[seed] Uploading DAGs from /opt/dags to s3://mwaa-dags-bucket/dags/"
  awslocal s3 sync /opt/dags/ s3://mwaa-dags-bucket/dags/ --exclude "*.pyc" --exclude "__pycache__/*" || true
else
  echo "[seed] No DAGs mounted at /opt/dags; skipping DAG upload."
fi

if [ -f /opt/mwaa-requirements.txt ]; then
  echo "[seed] Uploading requirements to s3://mwaa-dags-bucket/requirements.txt"
  awslocal s3 cp /opt/mwaa-requirements.txt s3://mwaa-dags-bucket/requirements.txt || true
fi

exit 0
