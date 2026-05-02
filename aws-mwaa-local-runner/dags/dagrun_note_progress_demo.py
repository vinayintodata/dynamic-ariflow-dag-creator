"""
Counter demo: each Python task +1 per second; every 5 seconds it writes **its** task note
with the current count; succeeds when the count reaches **1_000_000** (override for tests).

Env:
  DAGRUN_NOTE_DEMO_MAX_COUNT — success when count reaches this (default: 1000000).
  DAGRUN_NOTE_DEMO_TICK_SEC — seconds between +1 (default: 1).
  DAGRUN_NOTE_DEMO_NOTE_EVERY — update task note every N counts (default: 5 → every 5s if tick=1).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from textwrap import dedent

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

log = logging.getLogger(__name__)

DAG_ID = "dagrun_note_progress_demo"
TASK_NOTE_MAX_LEN = 995
DEFAULT_MAX_COUNT = 1_000_000
DEFAULT_TICK_SEC = 1.0
DEFAULT_NOTE_EVERY = 5


def _max_count() -> int:
    raw = os.environ.get("DAGRUN_NOTE_DEMO_MAX_COUNT", str(DEFAULT_MAX_COUNT))
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_MAX_COUNT


def _tick_sec() -> float:
    raw = os.environ.get("DAGRUN_NOTE_DEMO_TICK_SEC", str(DEFAULT_TICK_SEC))
    try:
        return max(0.001, float(raw))
    except ValueError:
        return DEFAULT_TICK_SEC


def _note_every() -> int:
    raw = os.environ.get("DAGRUN_NOTE_DEMO_NOTE_EVERY", str(DEFAULT_NOTE_EVERY))
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_NOTE_EVERY


def _set_this_task_note(ti: TaskInstance, body: str) -> None:
    text = body if len(body) <= TASK_NOTE_MAX_LEN else body[: TASK_NOTE_MAX_LEN - 3] + "..."
    with create_session() as session:
        tio = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
            )
            .one_or_none()
        )
        if tio is None:
            log.warning(
                "TaskInstance missing for note dag=%s task=%s run=%s",
                ti.dag_id,
                ti.task_id,
                ti.run_id,
            )
            return
        tio.note = text


def counting_task(**context) -> None:
    """
    +1 per tick (default 1s sleep). Every ``note_every`` counts, refresh this task's note
    with the current count. Success when count == max_count.
    """
    ti: TaskInstance = context["ti"]
    max_count = _max_count()
    tick = _tick_sec()
    every = _note_every()

    utc = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def render_note(current: int) -> str:
        return dedent(
            f"""\
            **count (max so far):** `{current}` / `{max_count}`

            _task:_ `{ti.task_id}` · _UTC:_ {utc()}\
            """
        ).strip()

    if max_count == 0:
        _set_this_task_note(ti, render_note(0))
        log.info("MAX_COUNT=0, exiting success")
        return

    count = 0
    _set_this_task_note(ti, render_note(0))

    while count < max_count:
        time.sleep(tick)
        count += 1
        if count % every == 0 or count >= max_count:
            _set_this_task_note(ti, render_note(count))

    log.info("counter finished at %s", count)


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
    tags=["demo", "task_note", "mwaa"],
    doc_md=dedent(
        f"""\
        #### Per-task counter → task notes

        Each task sleeps **{_tick_sec()}s** per +1. Every **{_note_every()}** counts (~5s with defaults),
        **that task's note** (Grid 📝 on the cell) shows:

        `count / {DEFAULT_MAX_COUNT}` (max target).

        Override for local runs: `DAGRUN_NOTE_DEMO_MAX_COUNT=60`, optional `DAGRUN_NOTE_DEMO_TICK_SEC`,
        `DAGRUN_NOTE_DEMO_NOTE_EVERY`.

        **Parallel tasks** each run their own counter to `MAX_COUNT`.
        """
    ).strip(),
) as dag:
    counter_a = PythonOperator(
        task_id="counter_a",
        python_callable=counting_task,
    )
    counter_b = PythonOperator(
        task_id="counter_b",
        python_callable=counting_task,
    )

    # Two independent counters in parallel; each succeeds at MAX_COUNT.
