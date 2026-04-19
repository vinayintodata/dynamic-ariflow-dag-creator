"""
Python callables for dag_a — migrated unchanged from the original DAG source.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
DATA_DIR = os.path.join(AIRFLOW_HOME, "data")
READY_FILE = os.path.join(DATA_DIR, "from_a", "ready.txt")


def hello_add(x: int, y: int) -> int:
    """Hello world: add two integers and log the result."""
    result = x + y
    print(f"hello world: {x} + {y} = {result}")
    return result


def pick_branch_after_sum(**context) -> str:
    """Branch: if sum is positive, run bash_ok; else bash_else."""
    ti = context["ti"]
    total = ti.xcom_pull(task_ids="hello_add")
    if total is None:
        return "branch_bash_else"
    if total > 0:
        return "branch_bash_ok"
    return "branch_bash_else"


def write_ready_file(path: str = READY_FILE) -> str:
    target_dir = os.path.dirname(path)
    log.info("write_ready_file: creating dir %s", target_dir)
    os.makedirs(target_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        ts = datetime.utcnow().isoformat()
        f.write(f"ready at {ts}Z\n")
    log.info("write_ready_file: wrote %s", path)
    return path
