"""Convenience query helpers for DuckDB-backed lineage graphs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from graph.lineage_graph import LineageGraph

DEFAULT_DUCKDB_PATH = "data/pipeline.duckdb"


def load_graph(duckdb_path: str = DEFAULT_DUCKDB_PATH, batch_id: Optional[str] = None) -> LineageGraph:
    graph = LineageGraph()
    if Path(duckdb_path).exists():
        graph.build_from_duckdb(duckdb_path=duckdb_path, batch_id=batch_id)
    return graph


def trace_ancestry(
    target_col: str,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> List[str]:
    return load_graph(duckdb_path, batch_id).ancestors(target_col)


def trace_impact(
    source_col: str,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> List[str]:
    return load_graph(duckdb_path, batch_id).descendants(source_col)


def path_between(
    source_col: str,
    target_col: str,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> List[str]:
    return load_graph(duckdb_path, batch_id).path_between(source_col, target_col)


def transforms_for_column(
    col: str,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> List[str]:
    return load_graph(duckdb_path, batch_id).transforms_touching(col)


def graph_summary(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    return load_graph(duckdb_path, batch_id).summary()


def graph_json(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    return json.loads(load_graph(duckdb_path, batch_id).to_json())
