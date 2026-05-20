"""Streamlit entry point for the lineage intelligence dashboard."""

from __future__ import annotations

from pathlib import Path
from typing import Any, List

import streamlit as st

DUCKDB_PATH = "data/pipeline.duckdb"


def query_table(sql: str, params: List[Any] | None = None) -> List[dict]:
    try:
        import duckdb
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = con.execute(sql, params or []).fetchall()
        cols = [desc[0] for desc in con.description]
        con.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception:
        return []


st.set_page_config(page_title="Lineage Intelligence", layout="wide")
st.title("Financial Lineage Intelligence")
st.caption("Local DuckDB, DQ, and column-level lineage observability.")

if not Path(DUCKDB_PATH).exists():
    st.warning("No DuckDB database found yet. Run the pipeline to populate dashboard data.")

runs = query_table(
    """
    SELECT batch_id, pipeline_name, stage_name, status, records_in, records_out,
           records_dead, duration_sec, started_at
    FROM pipeline_runs
    ORDER BY started_at DESC
    LIMIT 20
    """
)
scores = query_table(
    """
    SELECT batch_id, checkpoint, dq_score, total_violations, recorded_at
    FROM dq_scores
    ORDER BY recorded_at DESC
    LIMIT 20
    """
)
lineage = query_table("SELECT COUNT(*) AS edge_count FROM lineage_edges")

col1, col2, col3 = st.columns(3)
col1.metric("Recent Stage Runs", len(runs))
col2.metric("DQ Score Rows", len(scores))
col3.metric("Lineage Edges", lineage[0]["edge_count"] if lineage else 0)

st.subheader("Recent Pipeline Runs")
st.dataframe(runs, use_container_width=True)

st.subheader("Recent DQ Scores")
st.dataframe(scores, use_container_width=True)

st.info("Use the sidebar pages for run history, DQ drilldown, lineage exploration, and the AI advisor placeholder.")
