"""Small Streamlit helpers for rendering lineage data."""

from __future__ import annotations

from typing import Any, Dict

import streamlit as st


def render_lineage_summary(summary: Dict[str, Any]) -> None:
    col1, col2, col3 = st.columns(3)
    col1.metric("Nodes", summary.get("node_count", 0))
    col2.metric("Edges", summary.get("edge_count", 0))
    col3.metric("Transforms", len(summary.get("transforms", [])))


def render_graph_json(graph_data: Dict[str, Any]) -> None:
    st.json(graph_data)
