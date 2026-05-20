"""AI advisor placeholder backed by lineage context."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from graph.queries import graph_json

DUCKDB_PATH = "data/pipeline.duckdb"

st.title("AI Lineage Advisor")
st.caption("Ask lineage questions. Without an Anthropic API key, this page shows the graph context it would send.")

question = st.text_area("Question", value="Which upstream fields affect risk_report.eur_exposure?")
context = graph_json(DUCKDB_PATH)

if st.button("Ask"):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        st.warning("ANTHROPIC_API_KEY is not set. Showing lineage context instead of calling the API.")
        st.json(context)
    else:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
                max_tokens=1000,
                system="Answer questions about financial data lineage using only the provided graph context.",
                messages=[
                    {
                        "role": "user",
                        "content": f"Lineage graph: {context}\n\nQuestion: {question}",
                    }
                ],
            )
            st.write(response.content[0].text)
        except Exception as exc:
            st.error(f"AI advisor call failed: {exc}")
