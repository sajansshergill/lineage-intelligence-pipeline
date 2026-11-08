"""Column-level lineage explorer page."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from graph.queries import graph_json, graph_summary, path_between, trace_ancestry, trace_impact
from dashboard.data_bootstrap import ensure_demo_database

DUCKDB_PATH = ensure_demo_database()

st.set_page_config(page_title="Lineage Explorer · Lineage Intelligence", layout="wide")

st.title("Lineage Explorer")

batch_id = st.text_input("Batch ID (optional)", value="")
batch_filter = batch_id.strip() or None
summary = graph_summary(DUCKDB_PATH, batch_filter)

col1, col2, col3 = st.columns(3)
col1.metric("Nodes", summary.get("node_count", 0))
col2.metric("Edges", summary.get("edge_count", 0))
col3.metric("Batches", len(summary.get("batch_ids", [])))

st.write("Transforms:", ", ".join(summary.get("transforms", [])) or "None")

# --- 3D lineage graph: nodes layered by longest-path depth, drag to rotate ---
_graph = graph_json(DUCKDB_PATH, batch_filter)
_edges = _graph.get("edges", []) if isinstance(_graph, dict) else []
if _edges:
    _nodes = sorted({e["source"] for e in _edges} | {e["target"] for e in _edges})
    _depth = {n: 0 for n in _nodes}
    for _ in range(len(_nodes)):
        for _e in _edges:
            if _depth[_e["target"]] <= _depth[_e["source"]]:
                _depth[_e["target"]] = _depth[_e["source"]] + 1
    _by_depth: dict = {}
    for _n in _nodes:
        _by_depth.setdefault(_depth[_n], []).append(_n)
    _pos = {}
    for _d, _group in _by_depth.items():
        for _i, _n in enumerate(sorted(_group)):
            _pos[_n] = (float(_d), float(_i - (len(_group) - 1) / 2), float(_depth[_n] % 3))
    _traces = []
    for _e in _edges:
        _x0, _y0, _z0 = _pos[_e["source"]]
        _x1, _y1, _z1 = _pos[_e["target"]]
        _traces.append(go.Scatter3d(
            x=[_x0, _x1], y=[_y0, _y1], z=[_z0, _z1], mode="lines",
            line=dict(color="rgba(140,220,255,.65)", width=4),
            hoverinfo="text", text=f"{_e['source']} → {_e['target']} · {_e.get('transform', '')}",
            showlegend=False))
    _traces.append(go.Scatter3d(
        x=[_pos[n][0] for n in _nodes], y=[_pos[n][1] for n in _nodes],
        z=[_pos[n][2] for n in _nodes], mode="markers+text", text=_nodes,
        textposition="top center",
        marker=dict(size=9, color=[_depth[n] for n in _nodes], colorscale="Viridis",
                    line=dict(color="white", width=1)),
        hoverinfo="text", name="Columns"))
    _fig = go.Figure(data=_traces)
    _fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                       scene=dict(xaxis_title="Lineage depth", yaxis_title="Column",
                                  zaxis_title="Band"),
                       margin=dict(l=0, r=0, t=10, b=0),
                       title="🧊 Lineage graph in 3D — drag to rotate")
    st.plotly_chart(_fig, width="stretch")

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
