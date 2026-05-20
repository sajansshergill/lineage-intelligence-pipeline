"""DQ violation drilldown page."""

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
        st.info(f"No DQ data available yet: {exc}")
        return []


st.title("DQ Violations")

severity = st.selectbox("Severity", ["ALL", "CRITICAL", "ERROR", "WARN"])
where = "" if severity == "ALL" else "WHERE severity = ?"
params = [] if severity == "ALL" else [severity]

scores = query(
    """
    SELECT batch_id, checkpoint, dq_score, rules_run, rules_failed,
           total_violations, critical_count, error_count, warn_count, recorded_at
    FROM dq_scores
    ORDER BY recorded_at DESC
    LIMIT 100
    """
)
st.subheader("Scores")
st.dataframe(scores, use_container_width=True)

violations = query(
    f"""
    SELECT batch_id, rule_name, severity, field_name, offending_value,
           row_key, stage, ts
    FROM dq_violations
    {where}
    ORDER BY ts DESC
    LIMIT 500
    """,
    params,
)
st.subheader("Violations")
st.dataframe(violations, use_container_width=True)
