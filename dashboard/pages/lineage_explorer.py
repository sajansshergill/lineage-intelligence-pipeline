"""Column-level lineage explorer page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from graph.queries import graph_json, graph_summary, path_between, trace_ancestry, trace_impact
from dashboard.data_bootstrap import ensure_demo_database

DUCKDB_PATH = ensure_demo_database()

st.title("Lineage Explorer")

batch_id = st.text_input("Batch ID (optional)", value="")
batch_filter = batch_id.strip() or None
summary = graph_summary(DUCKDB_PATH, batch_filter)

col1, col2, col3 = st.columns(3)
col1.metric("Nodes", summary.get("node_count", 0))
col2.metric("Edges", summary.get("edge_count", 0))
col3.metric("Batches", len(summary.get("batch_ids", [])))

st.write("Transforms:", ", ".join(summary.get("transforms", [])) or "None")

target = st.text_input("Trace ancestry for column", value="risk_report.eur_exposure")
if st.button("Trace Ancestry"):
    st.write(trace_ancestry(target, DUCKDB_PATH, batch_filter))

source = st.text_input("Trace downstream impact from column", value="raw.notional")
if st.button("Trace Impact"):
    st.write(trace_impact(source, DUCKDB_PATH, batch_filter))

st.subheader("Path Between Columns")
left, right = st.columns(2)
path_source = left.text_input("Source", value="raw.notional")
path_target = right.text_input("Target", value="risk_report.eur_exposure")
if st.button("Find Path"):
    st.write(path_between(path_source, path_target, DUCKDB_PATH, batch_filter))

with st.expander("Raw Graph JSON"):
    st.json(graph_json(DUCKDB_PATH, batch_filter))
