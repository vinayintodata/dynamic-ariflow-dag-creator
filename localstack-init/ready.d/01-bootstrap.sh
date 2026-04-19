#!/bin/bash
# Idempotent bootstrap: IAM role, S3 bucket, Redshift cluster (LocalStack Redshift API on Hobby+).
set -u
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

echo "[bootstrap] Creating IAM role for MWAA-style execution..."
awslocal iam create-role --role-name airflow-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["airflow.amazonaws.com","airflow-env.amazonaws.com"]},"Action":"sts:AssumeRole"}]}' 2>/dev/null || true

awslocal iam put-role-policy --role-name airflow-role --policy-name airflow-inline \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*","logs:*","redshift:*","redshift-data:*","sts:*"],"Resource":"*"}]}' 2>/dev/null || true

echo "[bootstrap] Creating S3 bucket for DAGs and plugins..."
awslocal s3 mb s3://mwaa-dags-bucket 2>/dev/null || true

echo "[bootstrap] Creating Redshift cluster (API)..."
awslocal redshift create-cluster \
  --cluster-identifier local-redshift \
  --db-name dev \
  --master-username masteruser \
  --master-user-password 'LocalStackRedshift123!' \
  --node-type n1 2>/dev/null || true

# Poll briefly; LocalStack Redshift often becomes available quickly. Avoid a long silent loop.
if [ "${REDSHIFT_WAIT:-1}" != "1" ]; then
  echo "[bootstrap] REDSHIFT_WAIT=0; skipping Redshift availability poll."
  exit 0
fi

echo "[bootstrap] Waiting for Redshift cluster status (max ~60s, progress every 10s)..."
i=0
while [ "$i" -lt 30 ]; do
  STATUS=$(awslocal redshift describe-clusters --cluster-identifier local-redshift --query 'Clusters[0].ClusterStatus' --output text 2>/dev/null || echo "unknown")
  if [ "$STATUS" = "available" ]; then
    echo "[bootstrap] Redshift cluster is available."
    awslocal redshift describe-clusters --cluster-identifier local-redshift --output table || true
    exit 0
  fi
  i=$((i + 1))
  if [ $((i % 5)) -eq 0 ]; then
    echo "[bootstrap] ... still waiting (status=${STATUS}, attempt ${i}/30)"
  fi
  sleep 2
done

echo "[bootstrap] Warning: Redshift did not become available in time; continuing anyway. Set REDSHIFT_WAIT=0 to skip this wait."
exit 0
