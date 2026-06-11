# Context prompt: MWAA multi-environment (preprod + preprod_1, Option B)

Copy everything below the line into a new chat when you want the assistant to inherit this design without re-explaining.

---

## Problem

We run **Apache Airflow on AWS MWAA**. We integrate with **Snowflake**, **Redshift**, **dbt Cloud**, **Salesforce**, etc.

We already have roughly **100 DAGs** that target the **preprod** data plane (today they use connections such as Snowflake/redshift/other `conn_id`s pointing at the **preprod** database / cluster).

We need an additional isolated slice **`preprod_1`** backed by **new infrastructure** (e.g. `snowflake_preprod_1`, `*`\_preprod_1 connections) so that:

- Another **~100 DAGs** (or logically parallel copies) run against **preprod_1**.
- **The original 100 preprod DAGs must stay unchanged** and keep using the existing preprod connections.
- We remain on **the same MWAA environment** (**Option B**, not a second MWAA deployment).

Airflow **`dag_id` must be globally unique** in one MWAA, so we **cannot** run one DAG definition against two databases; we need **new DAG definitions** with unique `dag_id`s (recommended suffix convention: `__preprod_1` or `_p1`).

## How MWAA stores connections

- Connections live in Airflow **metadata RDS** behind MWAA (UI / API / CLI / optional import scripts), **not** in DAG files on S3.
- Passwords/tokens sit in **HashiCorp Vault** (KV / dynamic secrets as you standardize). Airflow resolves them via a **secrets backend** configured on MWAA (`AIRFLOW__SECRETS__BACKEND` etc.); see the [apache-airflow-providers-hashicorp](https://airflow.apache.org/docs/apache-airflow-providers-hashicorp/stable/index.html) docs (Vault backend class and connection parameters). MWAA workers must have **network reachability** to Vault and the **Hashicorp provider** in your environment `requirements.txt` (same pattern as other community providers on MWAA).
- DAG code only references **`conn_id` strings**.

## Operational checklist (Option B)

1. Create **new** MWAA Connections for preprod_1 (`snowflake_preprod_1`, `redshift_preprod_1`, HTTP/dbt as needed); **never overwrite** existing preprod connection rows.
2. Add **matching Vault entries** (paths or dynamic roles) for preprod_1 and bind them to those connection IDs via the Airflow–Vault secrets backend.
3. Deploy **additive** DAG code: duplicate logical pipelines with **new `dag_id`s** + preprod_1 `conn_id`s on every relevant operator/task.
4. Use **DAG tags** (e.g. `env:preprod_1`) for UI filtering and ownership.
5. Prefer **YAML manifest / codegen / iterator** over manually duplicating hundreds of DAG files so behavioral changes stay synchronized.

## Anti-patterns

- **Do not** use one global Airflow Variable to pick “current DB” for all DAGs — toggling it would retarget mass workloads unpredictably after parse.
- **Do not** put secrets inside DAG repos.

## Repo note (optional)

This project may use a **YAML → dag_factory loader** pattern; when generating preprod vs preprod_1, parameterized manifests should emit **paired DAGs** (different `dag_id`, tags, `conn_id`) rather than editing the originals.

---

## Ask the assistant to…

(Respond to one of these, depending on task.)

1. Draft **connection naming conventions** and a **Vault path / mount** layout (e.g. `kv/data/airflow/connections/snowflake_preprod_1`) for preprod vs preprod_1.
2. Sketch **YAML / loader changes** so one source produces both `my_dag` and `my_dag__preprod_1`.
3. Review **risk**: pools, parallelism, lineage, alerting, SLA duplication when doubling DAG counts in one MWAA.
