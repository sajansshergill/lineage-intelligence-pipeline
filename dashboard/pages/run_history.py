"""Run history dashboard page."""

from __future__ import annotations

from typing import Any, List

import streamlit as st

DUCKDB_PATH = "data/pipeline.duckdb"


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
