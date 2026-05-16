"""
etl/lineage_tracker.py
----------------------
Column-level lineage tracker.

Every transform calls tracker.emit(...) to record a directed edge:
    source_col -> target_col via transform_name at stage

After the pipeline completes, build_graph() assembles all edges into a
NetworkX DiGraph that can be:
  - Queried for ancestry / impact analysis
  - Serialized to JSON for the Streamlit dashboard
  - Persisted to Stardog via stardog_client.py (optional)

The tracker is thread-safe for use within a single driver process.
(PySpark UDFs run on executors — emit() calls must happen on the driver,
which is where BaseTransform._transform() runs for DataFrame transforms.)
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Edge dataclass
# ---------------------------------------------------------------------------

@dataclass
class LineageEdge:
    """
    A single column-level lineage edge.

    Fields:
        source:    Fully-qualified source column (e.g. "raw.notional")
        target:    Fully-qualified target column (e.g. "enriched.notional_usd")
        transform: Name of the registered transform that produced this edge
        stage:     Pipeline stage (ingest | clean | enrich | load)
        ts:        UTC timestamp of emission
        metadata:  Optional dict for extra context (fx_rate, null_count, etc.)
    """
    source:    str
    target:    str
    transform: str
    stage:     str
    ts:        str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata:  Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Lineage tracker
# ---------------------------------------------------------------------------

class LineageTracker:
    """
    Collects lineage edges during a pipeline run and builds a queryable graph.

    Usage in a transform:
        self.tracker.emit(
            source="clean.notional",
            target="enriched.notional_usd",
            transform="fx_convert",
            stage="enrich",
            metadata={"fx_rate": 1.085}
        )

    After the pipeline:
        graph = tracker.build_graph()
        ancestry = tracker.ancestors("enriched.notional_eur")
    """

    def __init__(self):
        self._edges: List[LineageEdge] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

    def emit(
        self,
        source: str,
        target: str,
        transform: str,
        stage: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a single lineage edge.

        Args:
            source:    Fully-qualified source column name.
            target:    Fully-qualified target column name.
            transform: Registered transform name.
            stage:     Pipeline stage identifier.
            metadata:  Optional dict of additional context.
        """
        edge = LineageEdge(
            source=source,
            target=target,
            transform=transform,
            stage=stage,
            metadata=metadata or {},
        )
        with self._lock:
            self._edges.append(edge)

        logger.debug(
            "LINEAGE | %s -> %s [%s @ %s]",
            source, target, transform, stage,
        )

    def merge(self, other: "LineageTracker") -> None:
        """Merge edges from another tracker (e.g. combine per-transform trackers)."""
        with self._lock:
            self._edges.extend(other._edges)

    @property
    def edges(self) -> List[LineageEdge]:
        with self._lock:
            return list(self._edges)

    def edge_count(self) -> int:
        return len(self._edges)

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def build_graph(self):
        """
        Assemble all emitted edges into a NetworkX DiGraph.

        Each node is a fully-qualified column name (e.g. "raw.notional").
        Each edge carries transform, stage, ts, and metadata as attributes.

        Returns:
            networkx.DiGraph
        """
        try:
            import networkx as nx
        except ImportError:
            raise ImportError(
                "networkx is required for lineage graph construction. "
                "Install with: pip install networkx"
            )

        graph = nx.DiGraph()

        for edge in self._edges:
            graph.add_node(edge.source)
            graph.add_node(edge.target)
            graph.add_edge(
                edge.source,
                edge.target,
                transform=edge.transform,
                stage=edge.stage,
                ts=edge.ts,
                metadata=edge.metadata,
            )

        logger.info(
            "Lineage graph built | nodes=%d edges=%d",
            graph.number_of_nodes(), graph.number_of_edges(),
        )
        return graph

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def ancestors(self, target_col: str) -> List[str]:
        """
        Return all columns that are upstream ancestors of target_col.

        Args:
            target_col: Fully-qualified column name (e.g. "risk_report.eur_exposure")

        Returns:
            List of ancestor column names in topological order (root first).
        """
        import networkx as nx
        graph = self.build_graph()
        if target_col not in graph:
            logger.warning("Column '%s' not found in lineage graph.", target_col)
            return []
        ancestors = list(nx.ancestors(graph, target_col))
        # Sort by approximate topological position (shortest path from source)
        return sorted(ancestors, key=lambda n: nx.shortest_path_length(graph, n, target_col))

    def descendants(self, source_col: str) -> List[str]:
        """
        Return all columns downstream of source_col (impact analysis).

        Args:
            source_col: Fully-qualified column name (e.g. "raw.notional")

        Returns:
            List of descendant column names.
        """
        import networkx as nx
        graph = self.build_graph()
        if source_col not in graph:
            logger.warning("Column '%s' not found in lineage graph.", source_col)
            return []
        return list(nx.descendants(graph, source_col))

    def path_between(self, source: str, target: str) -> List[str]:
        """
        Return the shortest lineage path between two columns.

        Returns:
            Ordered list of column names [source, ..., target],
            or empty list if no path exists.
        """
        import networkx as nx
        graph = self.build_graph()
        try:
            return nx.shortest_path(graph, source=source, target=target)
        except nx.NetworkXNoPath:
            return []
        except nx.NodeNotFound as exc:
            logger.warning("path_between: %s", exc)
            return []

    def transforms_touching(self, col: str) -> List[str]:
        """
        Return all transform names that produce or consume a given column.

        Args:
            col: Column name to search (matches source or target).

        Returns:
            Deduplicated list of transform names.
        """
        transforms = set()
        for edge in self._edges:
            if edge.source == col or edge.target == col:
                transforms.add(edge.transform)
        return sorted(transforms)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_json(self, indent: int = 2) -> str:
        """
        Serialize the full lineage graph to JSON for dashboard/API consumption.

        Returns:
            JSON string with nodes and edges lists.
        """
        import networkx as nx
        graph = self.build_graph()

        nodes = [
            {
                "id": node,
                "layer": node.split(".")[0] if "." in node else "unknown",
                "column": node.split(".")[-1],
            }
            for node in graph.nodes
        ]

        edges = [
            {
                "source": u,
                "target": v,
                **{k: v for k, v in attrs.items() if k != "metadata"},
                "metadata": attrs.get("metadata", {}),
            }
            for u, v, attrs in graph.edges(data=True)
        ]

        return json.dumps({"nodes": nodes, "edges": edges}, indent=indent, default=str)

    def save_json(self, path: str) -> None:
        """Write lineage graph JSON to a file."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())
        logger.info("Lineage graph saved to: %s", path)

    def to_edge_list(self) -> List[Dict[str, Any]]:
        """Return raw edge list as list of dicts (for DuckDB persistence)."""
        return [asdict(edge) for edge in self._edges]

    def save_to_duckdb(self, duckdb_path: str, batch_id: str) -> None:
        """
        Persist all lineage edges to DuckDB for historical querying.

        Args:
            duckdb_path: Path to the DuckDB file.
            batch_id:    Current batch identifier.
        """
        try:
            import duckdb
            con = duckdb.connect(duckdb_path)
            con.execute("""
                CREATE TABLE IF NOT EXISTS lineage_edges (
                    batch_id  VARCHAR,
                    source    VARCHAR,
                    target    VARCHAR,
                    transform VARCHAR,
                    stage     VARCHAR,
                    ts        VARCHAR,
                    metadata  VARCHAR
                )
            """)
            rows = [
                (
                    batch_id,
                    e.source,
                    e.target,
                    e.transform,
                    e.stage,
                    e.ts,
                    json.dumps(e.metadata),
                )
                for e in self._edges
            ]
            con.executemany(
                "INSERT INTO lineage_edges VALUES (?,?,?,?,?,?,?)", rows
            )
            con.close()
            logger.info(
                "Lineage edges persisted to DuckDB: %d rows | batch=%s",
                len(rows), batch_id,
            )
        except Exception as exc:
            logger.error("Failed to persist lineage to DuckDB: %s", exc)

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def print_summary(self) -> None:
        """Print a human-readable summary of all emitted edges."""
        print(f"\n{'='*60}")
        print(f"  Lineage Summary ({len(self._edges)} edges)")
        print(f"{'='*60}")
        for edge in self._edges:
            meta_str = f" | meta={edge.metadata}" if edge.metadata else ""
            print(f"  {edge.source:<35} -> {edge.target:<35} [{edge.transform} @ {edge.stage}]{meta_str}")
        print(f"{'='*60}\n")