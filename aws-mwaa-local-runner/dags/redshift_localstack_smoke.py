"""
Smoke test against LocalStack Redshift from Airflow (e.g. aws-mwaa-local-runner).

- describe_clusters works on Hobby+.
- Redshift Data API (execute_statement) requires Ultimate+ per LocalStack licensing.
  https://docs.localstack.cloud/aws/licensing/

Copy this file into aws-mwaa-local-runner's dags/ and set AWS_ENDPOINT_URL to reach LocalStack.
"""

from __future__ import annotations

import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

REGION = "us-east-1"
CLUSTER_ID = "local-redshift"
DB_NAME = "dev"
DB_USER = "masteruser"


def _endpoint_url() -> str:
    return os.environ.get("LOCALSTACK_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or "http://localstack:4566"


def _client(service: str):
    return boto3.client(
        service,
        region_name=REGION,
        endpoint_url=_endpoint_url(),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def resolve_cluster_host(address: str) -> str:
    """Inside Docker, loopback hostnames from DescribeClusters must point at the LocalStack service."""
    if address in ("127.0.0.1", "localhost"):
        return "localstack"
    return address


def describe_redshift():
    rs = _client("redshift")
    cluster = rs.describe_clusters(ClusterIdentifier=CLUSTER_ID)["Clusters"][0]
    ep = cluster["Endpoint"]
    host = resolve_cluster_host(ep["Address"])
    print(f"ClusterStatus={cluster.get('ClusterStatus')} Endpoint={host}:{ep.get('Port')}")
    return cluster


def optional_redshift_data_sql():
    """Uses Redshift Data API; enable on Ultimate+ LocalStack plans."""
    describe_redshift()
    rdata = _client("redshift-data")
    try:
        resp = rdata.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=DB_NAME,
            DbUser=DB_USER,
            Sql="SELECT 1 AS ok",
        )
        print(f"redshift-data Id={resp.get('Id')} Status={resp.get('Status')}")
    except Exception as exc:  # noqa: BLE001
        print(
            "redshift-data execute_statement failed (common on Hobby: Redshift Data API is Ultimate+): "
            f"{exc}"
        )


default_args = {
    "owner": "local",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="redshift_localstack_smoke",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["localstack", "redshift"],
) as dag:
    PythonOperator(task_id="describe_redshift", python_callable=describe_redshift)
    PythonOperator(task_id="optional_redshift_data_sql", python_callable=optional_redshift_data_sql)
