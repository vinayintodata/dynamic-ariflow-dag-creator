#!/usr/bin/env python3
"""
manifest_to_dag_factory.py
==========================
Reads YAML manifest files produced by the DAG metadata extractor and generates
a dag-factory compatible YAML configuration file.

Usage:
    python scripts/manifest_to_dag_factory.py \
        --manifest-dir migration_manifest_files/dag_manifests \
        --output     aws-mwaa-local-runner/dags/dag_factory_config.yml

The generated YAML is ready for dag-factory's ``load_yaml_dags`` to consume.
Python callables referenced by PythonOperator / BranchPythonOperator /
_PythonDecoratedOperator / PythonSensor tasks are resolved via
CALLABLE_REGISTRY below.  Add entries as you migrate more DAGs.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Any

import yaml


class _LiteralStr(str):
    """Mark a string for literal block-style output in YAML."""


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.Node:
    if isinstance(data, _LiteralStr):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def _dict_representer(dumper: yaml.Dumper, data: dict) -> yaml.Node:
    """Dump OrderedDict / dict preserving insertion order."""
    return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())


yaml.add_representer(str, _str_representer)
yaml.add_representer(_LiteralStr, _str_representer)
yaml.add_representer(OrderedDict, _dict_representer)
yaml.add_representer(dict, _dict_representer)


# ---------------------------------------------------------------------------
# Callable registry:  manifest callable name  ->  dotted import path
# The manifest stores e.g. ``<callable hello_add>`` — we strip the wrapper and
# look up the function here.
# ---------------------------------------------------------------------------
CALLABLE_REGISTRY: dict[str, str] = {
    # dag_a
    "hello_add":            "include.dag_a_callables.hello_add",
    "pick_branch_after_sum": "include.dag_a_callables.pick_branch_after_sum",
    "write_ready_file":     "include.dag_a_callables.write_ready_file",
    # dag_c
    "_ready_file_exists":   "include.dag_c_callables._ready_file_exists",
}

# Operator class -> dag-factory ``operator`` value
OPERATOR_MAP: dict[str, str] = {
    "EmptyOperator":           "airflow.providers.standard.operators.empty.EmptyOperator",
    "BashOperator":            "airflow.providers.standard.operators.bash.BashOperator",
    "PythonOperator":          "airflow.providers.standard.operators.python.PythonOperator",
    "BranchPythonOperator":    "airflow.providers.standard.operators.python.BranchPythonOperator",
    "_PythonDecoratedOperator": "airflow.providers.standard.operators.python.PythonOperator",
    "ExternalTaskSensor":      "airflow.providers.standard.sensors.external_task.ExternalTaskSensor",
    "PythonSensor":            "airflow.providers.standard.sensors.python.PythonSensor",
}

# Fields we propagate from the manifest task when they differ from defaults.
PASSTHROUGH_FIELDS = {
    "trigger_rule": "all_success",
    "retries": 0,
    "pool": "default_pool",
    "priority_weight": 1,
    "weight_rule": "downstream",
    "queue": "default",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "email_on_failure": True,
    "email_on_retry": True,
}


def _parse_callable_name(raw: str) -> str | None:
    """Extract function name from ``<callable foo>``."""
    m = re.match(r"<callable\s+(\w+)>", raw)
    return m.group(1) if m else None


def _resolve_callable(raw: str) -> str:
    name = _parse_callable_name(raw)
    if name and name in CALLABLE_REGISTRY:
        return CALLABLE_REGISTRY[name]
    raise ValueError(
        f"Unknown callable {raw!r} — add it to CALLABLE_REGISTRY in this script."
    )


def _convert_retry_delay(raw: str) -> int:
    """``0:05:00`` -> 300  (seconds, dag-factory expects timedelta or int)."""
    parts = raw.split(":")
    if len(parts) == 3:
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 3600 + m * 60 + s
    return 300


def _build_task(task_id: str, tdata: dict[str, Any],
                id_remap: dict[str, str]) -> dict[str, Any]:
    """Convert one manifest task entry into a dag-factory task dict.

    ``id_remap`` maps the original full task_id (e.g. ``extract.extract_1``)
    to the YAML key we use in the output (e.g. ``extract_1`` when inside a
    task group).  Dependencies are rewritten accordingly.
    """
    task_type = tdata["task_type"]
    op_key = OPERATOR_MAP.get(task_type)
    if op_key is None:
        raise ValueError(f"Unsupported task_type {task_type!r} for task {task_id}")

    out: dict[str, Any] = {"operator": op_key}

    # --- operator-specific args -------------------------------------------
    call_args = tdata.get("operator_call_args") or {}

    if task_type == "BashOperator":
        out["bash_command"] = call_args.get("bash_command", "echo no-op")

    elif task_type in ("PythonOperator", "BranchPythonOperator",
                       "_PythonDecoratedOperator"):
        raw_callable = call_args.get("python_callable", "")
        out["python_callable"] = _resolve_callable(raw_callable)
        op_kwargs = call_args.get("op_kwargs")
        if op_kwargs:
            out["op_kwargs"] = {k: v for k, v in op_kwargs.items() if v is not None}
        op_args = call_args.get("op_args")
        if op_args:
            out["op_args"] = list(op_args)

    elif task_type == "ExternalTaskSensor":
        out["external_dag_id"] = call_args.get("external_dag_id")
        out["external_task_id"] = call_args.get("external_task_id")
        if tdata.get("reschedule"):
            out["mode"] = "reschedule"
        out["timeout"] = 21600
        out["poke_interval"] = 30
        out["allowed_states"] = ["success"]
        out["failed_states"] = ["failed", "skipped", "upstream_failed"]

    elif task_type == "PythonSensor":
        raw_callable = call_args.get("python_callable", "")
        out["python_callable"] = _resolve_callable(raw_callable)
        if tdata.get("reschedule"):
            out["mode"] = "reschedule"
        out["timeout"] = 21600
        out["poke_interval"] = 15

    # --- trigger_rule (only if non-default) --------------------------------
    tr = tdata.get("trigger_rule", "all_success")
    if tr != "all_success":
        out["trigger_rule"] = tr

    # --- dependencies (upstream), remapped to output keys ------------------
    upstream = tdata.get("upstream_task_ids", [])
    if upstream:
        out["dependencies"] = [id_remap.get(u, u) for u in upstream]

    # --- task group placement ----------------------------------------------
    placement = tdata.get("placement", {})
    tg = placement.get("task_group")
    if tg:
        out["task_group_name"] = tg

    return out


def _collect_task_groups(tasks: dict[str, dict]) -> dict[str, dict]:
    """Gather task_group definitions from task placements."""
    groups: dict[str, dict] = {}
    for tdata in tasks.values():
        placement = tdata.get("placement", {})
        tg = placement.get("task_group")
        if tg and tg not in groups:
            groups[tg] = {"tooltip": f"Task group: {tg}"}
    return groups


def convert_dag(manifest: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Convert a single DAG manifest into a dag-factory DAG config dict."""
    dag_section = manifest["dag"]
    dag_id = dag_section["dag_id"]

    dag_cfg: dict[str, Any] = OrderedDict()

    dag_cfg["default_args"] = {
        "owner": "airflow",
        "start_date": dag_section.get("start_date", "2025-01-01"),
    }
    dag_cfg["schedule"] = dag_section.get("schedule", "@daily")
    dag_cfg["catchup"] = dag_section.get("catchup", False)
    dag_cfg["tags"] = dag_section.get("tags", [])

    tasks_data = manifest.get("tasks", {})

    # task groups
    tgroups = _collect_task_groups(tasks_data)
    if tgroups:
        dag_cfg["task_groups"] = tgroups

    # Build id_remap: full manifest task_id -> YAML output key.
    # For grouped tasks the key is the local_task_id (without group prefix);
    # dag-factory prepends the group automatically.
    # For root tasks the key stays the same.
    id_remap: dict[str, str] = {}
    for tid, tdata in tasks_data.items():
        placement = tdata.get("placement", {})
        local_id = placement.get("local_task_id", tid)
        tg = placement.get("task_group")
        yaml_key = local_id if tg else tid
        id_remap[tid] = yaml_key

    # tasks (preserve topological order from manifest)
    topo_order = manifest.get("global_topological_order", list(tasks_data.keys()))
    tasks_out: dict[str, Any] = OrderedDict()
    for tid in topo_order:
        if tid not in tasks_data:
            continue
        tdata = tasks_data[tid]
        yaml_key = id_remap[tid]
        tasks_out[yaml_key] = _build_task(tid, tdata, id_remap)

    dag_cfg["tasks"] = tasks_out

    return dag_id, dict(dag_cfg)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert DAG manifest YAMLs to a dag-factory config YAML."
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        required=True,
        help="Directory containing per-DAG manifest YAML files and index.yaml.",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Output path for the dag-factory YAML config.",
    )
    args = parser.parse_args()

    manifest_dir: Path = args.manifest_dir
    if not manifest_dir.is_dir():
        print(f"ERROR: {manifest_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    index_path = manifest_dir / "index.yaml"
    if not index_path.exists():
        print(f"ERROR: {index_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(index_path, "r", encoding="utf-8") as f:
        index = yaml.safe_load(f)

    dag_ids = index["export"]["dag_ids"]
    print(f"Found {len(dag_ids)} DAGs in index: {dag_ids}")

    all_dags: dict[str, Any] = OrderedDict()

    for dag_id in dag_ids:
        manifest_path = manifest_dir / f"{dag_id}.yaml"
        if not manifest_path.exists():
            print(f"  WARN: {manifest_path} not found, skipping {dag_id}",
                  file=sys.stderr)
            continue

        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = yaml.safe_load(f)

        did, cfg = convert_dag(manifest)
        all_dags[did] = cfg
        task_count = len(cfg.get("tasks", {}))
        group_count = len(cfg.get("task_groups", {}))
        print(f"  {did}: {task_count} tasks, {group_count} task groups")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        yaml.dump(
            dict(all_dags),
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    print(f"\nWrote dag-factory config to {args.output}")
    print("Done.")


if __name__ == "__main__":
    main()
