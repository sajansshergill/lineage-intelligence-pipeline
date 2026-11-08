"""Run history dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard.data_bootstrap import ensure_demo_database

DUCKDB_PATH = ensure_demo_database()

st.set_page_config(page_title="Run History · Lineage Intelligence", layout="wide")


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

if runs:
    df = pd.DataFrame(runs)
    c1, c2, c3 = st.columns(3)
    c1.metric("Stage runs", len(df))
    c2.metric("Records in", f"{int(df['records_in'].sum()):,}",
              delta=f"{int(df['records_dead'].sum()):,} dead-lettered")
    c3.metric("Total duration", f"{float(df['duration_sec'].sum()):.1f}s",
              delta=f"{df['status'].eq('SUCCESS').mean() * 100:.0f}% success")
    fig = px.bar(df, x="stage_name", y=["records_in", "records_out", "records_dead"],
                 barmode="group", template="plotly_dark",
                 title="Records in / out / dead-letter by stage")
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, width="stretch")

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
