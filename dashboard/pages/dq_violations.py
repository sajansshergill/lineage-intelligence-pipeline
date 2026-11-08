"""DQ violation drilldown page."""

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

st.set_page_config(page_title="DQ Violations · Lineage Intelligence", layout="wide")


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
if scores:
    sdf = pd.DataFrame(scores)
    fig = px.bar(sdf.iloc[::-1], x="checkpoint", y="dq_score", color="checkpoint",
                 text="dq_score", template="plotly_dark",
                 title="DQ score by checkpoint")
    fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
    fig.update_yaxes(range=[0, 115])
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, width="stretch")
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
if violations:
    vdf = pd.DataFrame(violations)
    left, right = st.columns(2)
    with left:
        sev = vdf["severity"].value_counts().reset_index()
        sev.columns = ["severity", "violations"]
        fig = px.bar(sev, x="severity", y="violations", color="severity",
                     template="plotly_dark", title="Violations by severity")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
    with right:
        by_rule = vdf["rule_name"].value_counts().reset_index()
        by_rule.columns = ["rule_name", "violations"]
        fig = px.bar(by_rule, x="violations", y="rule_name", orientation="h",
                     template="plotly_dark", title="Violations by rule")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
    st.download_button("⬇️ Export violations (CSV)", vdf.to_csv(index=False).encode(),
                       file_name="dq_violations.csv", mime="text/csv")
st.dataframe(violations, use_container_width=True)
