"""Run history dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard.data_bootstrap import ensure_demo_database

DUCKDB_PATH = ensure_demo_database()


def query(sql: str, params: List[Any] | None = None) -> List[dict]:
    try:
        import duckdb
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = con.execute(sql, params or []).fetchall()
        cols = [desc[0] for desc in con.description]
        con.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        st.info(f"No run history available yet: {exc}")
        return []


st.title("Run History")

runs = query(
    """
    SELECT batch_id, pipeline_name, stage_name, status, records_in, records_out,
           records_dead, duration_sec, error, started_at
    FROM pipeline_runs
    ORDER BY started_at DESC, stage_name
    LIMIT 200
    """
)
st.dataframe(runs, use_container_width=True)

jobs = query(
    """
    SELECT batch_id, job_name, status, started_at, finished_at, duration_sec,
           attempt, exit_code, error
    FROM job_runs
    ORDER BY started_at DESC
    LIMIT 200
    """
)
st.subheader("Job Runs")
st.dataframe(jobs, use_container_width=True)
