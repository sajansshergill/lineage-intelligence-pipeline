"""Reusable DQ scorecard component."""

from __future__ import annotations

from typing import Dict, Iterable

import streamlit as st


def render_scorecard(scores: Iterable[Dict]) -> None:
    rows = list(scores)
    latest = rows[0] if rows else {}
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest DQ Score", latest.get("dq_score", "n/a"))
    col2.metric("Violations", latest.get("total_violations", 0))
    col3.metric("Failed Rules", latest.get("rules_failed", 0))
    st.dataframe(rows, use_container_width=True)

