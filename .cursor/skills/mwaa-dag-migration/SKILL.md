---
name: mwaa-dag-migration
description: >-
  Build and manage Airflow DAGs dynamically from YAML manifest files using the
  dag_factory_loader. Use when creating new DAGs, modifying YAML configs,
  adding Python callables, debugging DAG load errors, deploying to MWAA,
  or working with the manifest-to-dag-factory conversion pipeline.
---

# MWAA DAG Migration Skill

## Project overview

This project migrates Airflow DAGs from a legacy system to AWS MWAA using a
declarative YAML-driven approach. DAGs are defined in YAML config files and
built at runtime by a generic Python loader — no per-DAG Python boilerplate.

## Key files

| File | Role |
|------|------|
| `aws-mwaa-local-runner/dags/dag_factory_loader.py` | Generic loader: scans for YAML, builds DAGs |
| `aws-mwaa-local-runner/dags/dag_factory_config.yml` | Generated DAG definitions |
| `aws-mwaa-local-runner/dags/include/` | Python callables (business logic) |
| `scripts/manifest_to_dag_factory.py` | Converts legacy manifests to dag_factory YAML |
| `migration_manifest_files/dag_manifests/` | Source manifests from old system |

## YAML DAG config format

```yaml
my_dag_id:
  default_args:
    owner: airflow
    start_date: '2025-01-01'
  schedule: '@daily'
  catchup: false
  tags: [example]
  task_groups:
    group_name:
      tooltip: 'Description'
  tasks:
    task_name:
      operator: airflow.providers.standard.operators.python.PythonOperator
      python_callable: include.module_name.function_name
      op_kwargs:
        key: value
      dependencies: [upstream_task]
      task_group_name: group_name
```

### Reserved keys (not passed to operator)

- `operator` — dotted import path for the operator class
- `dependencies` — list of upstream task IDs
- `task_group_name` — assigns task to a TaskGroup

All other keys are forwarded directly to the operator constructor.

### Auto-coerced keys

- `start_date`, `end_date` — parsed to `datetime`
- `retry_delay`, `execution_timeout`, `timeout`, `sla` — parsed to `timedelta` (accepts seconds or `"2h30m"` format)
- `python_callable`, `on_failure_callback`, etc. — imported from dotted path string
- `trigger_rule` — converted to `TriggerRule` enum

## Adding a new DAG

1. Create a `.yml` or `.yaml` file anywhere under `aws-mwaa-local-runner/dags/`
2. The loader discovers it automatically (recursive glob)
3. If it needs Python callables, add them under `dags/include/` with `__init__.py` in each subfolder
4. Reference callables as `include.module.function` in the YAML

## Adding new Python callables

1. Create a module in `aws-mwaa-local-runner/dags/include/`
2. Use `logging.getLogger(__name__)` for task-level log output
3. Use `os.environ.get("AIRFLOW_HOME")` instead of hardcoded paths
4. Reference the function in YAML as `include.module_name.function_name`

## Airflow 2.x / 3.x compatibility

The loader handles version differences automatically:

- Operator paths: `airflow.providers.standard.operators.*` auto-falls back to `airflow.operators.*` (and vice versa)
- `TaskGroup`: tries `airflow.sdk.TaskGroup` then `airflow.utils.task_group.TaskGroup`
- `TriggerRule`: tries `airflow.task.trigger_rule` then `airflow.utils.trigger_rule`

Always write YAML with Airflow 3.x paths (`airflow.providers.standard.*`); the loader resolves the rest.

## Running locally

```bash
cd aws-mwaa-local-runner
docker compose -f docker/docker-compose-local.yml -p mwaa_local up -d
```

- Airflow 2.x (MWAA): http://localhost:8080 — `admin` / `test`

Verify:

```bash
docker exec mwaa_local-local-runner-1 airflow dags list
docker exec mwaa_local-local-runner-1 airflow dags list-import-errors
docker exec mwaa_local-local-runner-1 airflow tasks test <dag_id> <task_id> 2025-01-01
```

## Regenerating YAML from manifests

```bash
python scripts/manifest_to_dag_factory.py
```

Reads `migration_manifest_files/dag_manifests/` and writes to
`aws-mwaa-local-runner/dags/dag_factory_config.yml`.

## Deploying to production MWAA

Upload `aws-mwaa-local-runner/dags/` contents to the S3 dags prefix:

```
s3://bucket/dags/
  dag_factory_loader.py
  dag_factory_config.yml
  include/
    __init__.py
    *.py
```

No extra pip dependencies needed — only `pyyaml` (bundled with Airflow).

## Common issues

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: No module named 'airflow.providers.standard'` | You're on Airflow 2.x — the loader handles this automatically |
| `PermissionError` writing files | Use `AIRFLOW_HOME` env var, not hardcoded paths |
| DAGs not showing in UI | Check `airflow dags list-import-errors` |
| YAML changes not picked up | Clear `__pycache__/` and restart the scheduler/webserver |
