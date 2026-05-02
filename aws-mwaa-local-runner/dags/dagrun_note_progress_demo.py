"""
DagRun note demo for MWAA / Airflow Grid View.

Shows the sticky-note control (📝) on a DagRun by updating ``dag_run.set_note()``
every 30 seconds during a long-running task with a Markdown table (mock progress).

Optional env (local testing):
  DAGRUN_NOTE_DEMO_RUNNER_SEC — total runner duration in seconds (default: 600).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from textwrap import dedent

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

log = logging.getLogger(__name__)

DAG_ID = "dagrun_note_progress_demo"
NOTE_INTERVAL_SEC = 30
DEFAULT_RUNNER_SEC = 600


def _runner_duration_sec() -> int:
    raw = os.environ.get("DAGRUN_NOTE_DEMO_RUNNER_SEC", str(DEFAULT_RUNNER_SEC))
    try:
        return max(NOTE_INTERVAL_SEC, int(raw))
    except ValueError:
        return DEFAULT_RUNNER_SEC


def _progress_markdown(completed: int, failed: int, label: str, elapsed_sec: int) -> str:
    return dedent(
        f"""\
        ### {label}

        | Tasks Completed | Tasks Failed |
        | ---: | ---: |
        | {completed} | {failed} |

        _Elapsed ~{elapsed_sec}s · UTC {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}_
        """
    ).strip()


def _set_note_on_dag_run(dag_id: str, run_id: str, note: str) -> None:
    """Persist note from a background thread (fresh DB session)."""
    with create_session() as session:
        dr = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
            .one_or_none()
        )
        if dr is None:
            log.warning("DagRun not found dag_id=%s run_id=%s", dag_id, run_id)
            return
        dr.set_note(note)


def mock_long_runner(**context) -> None:
    """
    Simulates ~10 minutes of work while a background loop calls ``set_note`` every 30s.
    """
    dag_run: DagRun = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    total_sec = _runner_duration_sec()

    # Seed note from task context (visible immediately in UI).
    dag_run.set_note(
        _progress_markdown(
            completed=0,
            failed=0,
            label="Mock runner — starting",
            elapsed_sec=0,
        )
    )

    stop = threading.Event()
    mock_total_units = max(1, total_sec // NOTE_INTERVAL_SEC)

    def _note_loop() -> None:
        start = time.monotonic()
        while not stop.wait(NOTE_INTERVAL_SEC):
            elapsed = int(time.monotonic() - start)
            # Mock ramp: "completed" steps increase each interval; optional failure tick for demo.
            completed = min(mock_total_units, 1 + elapsed // NOTE_INTERVAL_SEC)
            failed = (
                1
                if elapsed > total_sec // 2 and completed < mock_total_units // 3
                else 0
            )
            note = _progress_markdown(
                completed=completed,
                failed=failed,
                label="Live progress (mock)",
                elapsed_sec=elapsed,
            )
            _set_note_on_dag_run(dag_id, run_id, note)
            log.info("Updated DagRun note (mock completed=%s failed=%s)", completed, failed)

    ticker = threading.Thread(target=_note_loop, name="dagrun-note-ticker", daemon=True)
    ticker.start()
    try:
        log.info("Mock runner sleeping %s seconds (note updates every %s s)", total_sec, NOTE_INTERVAL_SEC)
        time.sleep(total_sec)
    finally:
        stop.set()
        ticker.join(timeout=NOTE_INTERVAL_SEC + 5)

    _set_note_on_dag_run(
        dag_id,
        run_id,
        _progress_markdown(
            completed=mock_total_units,
            failed=0,
            label="Mock runner — finished",
            elapsed_sec=total_sec,
        ),
    )


def monitor_pull_context(**context) -> None:
    """
    Second task: reads Airflow context, logs a snapshot, and writes a final DagRun note.
    """
    dag_run: DagRun = context["dag_run"]
    ti = context["ti"]
    snapshot = {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "logical_date": str(context.get("logical_date") or context.get("data_interval_start")),
        "task_id": ti.task_id,
        "try_number": ti.try_number,
        "map_index": ti.map_index,
    }
    log.info("Monitor context snapshot: %s", snapshot)

    # Final dashboard row: both this DAG's tasks succeeded in a normal run.
    dag_run.set_note(
        _progress_markdown(
            completed=2,
            failed=0,
            label="Run finished — monitor task",
            elapsed_sec=_runner_duration_sec(),
        )
        + "\n\n**Context (JSON keys):** `" + "`, `".join(sorted(context.keys())) + "`"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "dag_run_note", "mwaa"],
    doc_md=dedent(
        """\
        #### DagRun `set_note()` demo

        1. Trigger the DAG and open **Grid** view for this run.
        2. Click the **note** icon on the DagRun to see the Markdown table update about every 30s
           while **mock_long_runner** is running.
        3. **monitor_pull_context** logs context fields and sets a final note.

        Set `DAGRUN_NOTE_DEMO_RUNNER_SEC` (seconds) to shorten the long task locally.
        """
    ).strip(),
) as dag:
    run_mock = PythonOperator(
        task_id="mock_long_runner",
        python_callable=mock_long_runner,
    )
    run_monitor = PythonOperator(
        task_id="monitor_pull_context",
        python_callable=monitor_pull_context,
    )
    run_mock >> run_monitor
