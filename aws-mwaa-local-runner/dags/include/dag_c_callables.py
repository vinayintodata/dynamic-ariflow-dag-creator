"""
Python callables for dag_c — migrated unchanged from the original DAG source.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

import tempfile

DATA_DIR = os.path.join(tempfile.gettempdir(), "airflow_data")
READY_FILE = os.path.join(DATA_DIR, "from_a", "ready.txt")


def _ready_file_exists() -> bool:
    exists = os.path.exists(READY_FILE)
    log.info("_ready_file_exists: checking %s -> %s", READY_FILE, exists)
    return exists
