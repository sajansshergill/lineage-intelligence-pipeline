"""
graph/lineage_graph.py
----------------------
Builds, queries, and persists the column-level lineage graph.

This module is the graph layer that sits above etl/lineage_tracker.py.
While the tracker collects raw edges during a pipeline run, this module:
    1. Assembles edges into a persistent NetworkX DiGraph
    2. Loads historical edges from DuckDB across multiple batches
    3. Provides rich query methods used by the Streamlit dashboard
    4. Exports the graph to JSON for the Claude API advisor tab
    5. Optionally persists to Stardog via graph/stardog_client.py

Integrates with:
    etl/lineage_tracker.py      -> source of LineageEdge objects
    graph/queries.py            -> prebuilt query functions called by dashboard
    graph/stardog_client.py     -> optional RDF/SPARQL persistence
    dashboard/pages/
        lineage_explorer.py     -> calls build_from_duckdb(), to_json()
        ai_advisor.py           -> calls to_json() for Claude API context
    models/star_schema.sql      -> node names align with star schema columns
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LineageGraph:
    """
    Persistent, queryable column-level lineage graph backed by NetworkX.

    Can be populated three ways:
        1. From a LineageTracker after a live pipeline run
        2. From DuckDB lineage_edges table (historical, multi-batch)
        3. From a saved JSON file

    Node naming convention:
        <layer>.<column>
        e.g. raw.notional | clean.notional | enriched.notional_usd |
             risk_report.usd_exposure | fact_trades.notional_usd

    Layers in order:
        raw -> clean -> typed -> dedup -> enriched -> risk_report -> fact_trades
    """

    LAYER_ORDER = [
        "raw", "clean", "typed", "dedup",
        "enriched", "risk_report", "fact_trades",
    ]

    def __init__(self):
        try:
            import networkx as nx
            self._graph = nx.DiGraph()
        except ImportError:
            raise ImportError(
                "networkx is required. Install with: pip install networkx"
            )
        self._batch_ids: List[str] = []

    # ------------------------------------------------------------------
    # Population methods
    # ------------------------------------------------------------------

    def add_from_tracker(self, tracker, batch_id: str) -> None:
        """
        Add edges from a live LineageTracker into this graph.

        Args:
            tracker:  etl.lineage_tracker.LineageTracker instance.
            batch_id: Batch identifier for provenance tagging.
        """
        for edge in tracker.edges:
            self._add_edge(
                source=edge.source,
                target=edge.target,
                transform=edge.transform,
                stage=edge.stage,
                batch_id=batch_id,
                metadata=edge.metadata,
            )
        if batch_id not in self._batch_ids:
            self._batch_ids.append(batch_id)

        logger.info(
            "LineageGraph | added %d edges from tracker | batch=%s | "
            "total_nodes=%d total_edges=%d",
            len(tracker.edges), batch_id,
            self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    def build_from_duckdb(
        self,
        duckdb_path: str,
        batch_id: Optional[str] = None,
    ) -> None:
        """
        Load lineage edges from DuckDB lineage_edges table.
        Useful for rebuilding the graph for historical analysis or
        after a dashboard restart.

        Args:
            duckdb_path: Path to the DuckDB file.
            batch_id:    If provided, load only edges from this batch.
                         If None, load all edges across all batches.
        """
        try:
            import duckdb

            con = duckdb.connect(duckdb_path, read_only=True)
            params = [batch_id] if batch_id else []
            where = "WHERE batch_id = ?" if batch_id else ""
            rows = con.execute(
                f"""
                SELECT source, target, transform, stage, batch_id, metadata
                FROM lineage_edges
                {where}
                ORDER BY ts ASC
                """,
                params,
            ).fetchall()
            con.close()

            for source, target, transform, stage, b_id, metadata_str in rows:
                metadata = {}
                if metadata_str:
                    try:
                        metadata = json.loads(metadata_str)
                    except Exception:
                        pass
                self._add_edge(
                    source=source,
                    target=target,
                    transform=transform,
                    stage=stage,
                    batch_id=b_id,
                    metadata=metadata,
                )
                if b_id not in self._batch_ids:
                    self._batch_ids.append(b_id)

            logger.info(
                "LineageGraph | loaded %d edges from DuckDB | "
                "nodes=%d edges=%d",
                len(rows),
                self._graph.number_of_nodes(),
                self._graph.number_of_edges(),
            )

        except Exception as exc:
            logger.error("LineageGraph.build_from_duckdb() failed: %s", exc)

    def load_json(self, path: str) -> None:
        """
        Load a previously saved lineage graph from a JSON file.

        Args:
            path: Path to the JSON file written by save_json().
        """
        with open(path) as f:
            data = json.load(f)

        for edge in data.get("edges", []):
            self._add_edge(
                source=edge["source"],
                target=edge["target"],
                transform=edge.get("transform", "unknown"),
                stage=edge.get("stage", "unknown"),
                batch_id=edge.get("batch_id", "unknown"),
                metadata=edge.get("metadata", {}),
            )

        logger.info(
            "LineageGraph | loaded from JSON '%s' | nodes=%d edges=%d",
            path,
            self._graph.number_of_nodes(),
            self._graph.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Query methods (called by graph/queries.py and dashboard)
    # ------------------------------------------------------------------

    def ancestors(self, target_col: str) -> List[str]:
        """
        Return all upstream columns that contribute to target_col,
        sorted root-first by shortest path length.

        Args:
            target_col: Fully-qualified column (e.g. "risk_report.eur_exposure")

        Returns:
            Ordered list of ancestor column names.
        """
        import networkx as nx

        if target_col not in self._graph:
            logger.warning("ancestors(): '%s' not in graph.", target_col)
            return []

        ancs = list(nx.ancestors(self._graph, target_col))
        return sorted(
            ancs,
            key=lambda n: nx.shortest_path_length(self._graph, n, target_col),
            reverse=True,
        )

    def descendants(self, source_col: str) -> List[str]:
        """
        Return all downstream columns impacted by source_col.

        Args:
            source_col: Fully-qualified column (e.g. "raw.notional")

        Returns:
            List of descendant column names.
        """
        import networkx as nx

        if source_col not in self._graph:
            logger.warning("descendants(): '%s' not in graph.", source_col)
            return []

        return list(nx.descendants(self._graph, source_col))

    def path_between(self, source: str, target: str) -> List[str]:
        """
        Return the shortest lineage path between two columns.

        Returns:
            Ordered list [source, ..., target] or empty list if no path.
        """
        import networkx as nx

        try:
            return nx.shortest_path(self._graph, source=source, target=target)
        except (nx.NetworkXNoPath, nx.NodeNotFound) as exc:
            logger.warning("path_between('%s', '%s'): %s", source, target, exc)
            return []

    def transforms_touching(self, col: str) -> List[str]:
        """
        Return all transform names that produce or consume a given column.

        Args:
            col: Fully-qualified column name.

        Returns:
            Sorted deduplicated list of transform names.
        """
        transforms = set()
        for u, v, attrs in self._graph.edges(data=True):
            if u == col or v == col:
                transforms.add(attrs.get("transform", "unknown"))
        return sorted(transforms)

    def impact_analysis(self, source_col: str) -> Dict[str, Any]:
        """
        Full downstream impact report for a given source column.
        Used by the dashboard when a user asks "what breaks if X changes?"

        Args:
            source_col: Column to analyze.

        Returns:
            Dict with keys: source, descendants, transforms, path_count.
        """
        desc = self.descendants(source_col)
        transforms = set()
        for col in [source_col] + desc:
            transforms.update(self.transforms_touching(col))

        return {
            "source":      source_col,
            "descendants": desc,
            "transforms":  sorted(transforms),
            "path_count":  len(desc),
        }

    def nodes_by_layer(self) -> Dict[str, List[str]]:
        """
        Group all graph nodes by their layer prefix.

        Returns:
            Dict mapping layer name -> list of column names in that layer.
            Ordered by LAYER_ORDER.
        """
        result: Dict[str, List[str]] = {layer: [] for layer in self.LAYER_ORDER}
        result["other"] = []

        for node in self._graph.nodes:
            parts = node.split(".", 1)
            layer = parts[0] if len(parts) == 2 else "other"
            if layer in result:
                result[layer].append(node)
            else:
                result["other"].append(node)

        return result

    def summary(self) -> Dict[str, Any]:
        """
        Return a high-level summary of the graph for the dashboard header.

        Returns:
            Dict with node_count, edge_count, layers, batch_ids, transforms.
        """
        transforms = {
            attrs.get("transform", "unknown")
            for _, _, attrs in self._graph.edges(data=True)
        }
        return {
            "node_count":  self._graph.number_of_nodes(),
            "edge_count":  self._graph.number_of_edges(),
            "layers":      [l for l in self.LAYER_ORDER if l in self.nodes_by_layer() and self.nodes_by_layer()[l]],
            "batch_ids":   self._batch_ids,
            "transforms":  sorted(transforms),
        }

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_json(self, indent: int = 2) -> str:
        """
        Serialize the full graph to JSON for dashboard and Claude API use.

        Format:
            {
                "nodes": [{"id": "raw.notional", "layer": "raw", "column": "notional"}],
                "edges": [{"source": ..., "target": ..., "transform": ..., "stage": ...}]
            }
        """
        nodes = [
            {
                "id":     node,
                "layer":  node.split(".")[0] if "." in node else "unknown",
                "column": node.split(".")[-1],
            }
            for node in self._graph.nodes
        ]

        edges = []
        for u, v, attrs in self._graph.edges(data=True):
            edges.append({
                "source":    u,
                "target":    v,
                "transform": attrs.get("transform", "unknown"),
                "stage":     attrs.get("stage", "unknown"),
                "batch_id":  attrs.get("batch_id", "unknown"),
                "metadata":  attrs.get("metadata", {}),
            })

        return json.dumps(
            {"nodes": nodes, "edges": edges},
            indent=indent,
            default=str,
        )

    def save_json(self, path: str) -> None:
        """Write the serialized graph to a JSON file."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())
        logger.info("LineageGraph saved to: %s", path)

    def to_cytoscape(self) -> Dict[str, Any]:
        """
        Export graph in Cytoscape.js format for the Streamlit
        lineage explorer (rendered via streamlit-agraph or pyvis).

        Returns:
            Dict with 'nodes' and 'edges' lists in Cytoscape format.
        """
        layer_colors = {
            "raw":         "#6B7280",
            "clean":       "#3B82F6",
            "typed":       "#8B5CF6",
            "dedup":       "#EC4899",
            "enriched":    "#10B981",
            "risk_report": "#F59E0B",
            "fact_trades": "#EF4444",
            "other":       "#9CA3AF",
        }

        nodes = []
        for node in self._graph.nodes:
            layer = node.split(".")[0] if "." in node else "other"
            nodes.append({
                "data": {
                    "id":    node,
                    "label": node.split(".")[-1],
                    "layer": layer,
                    "color": layer_colors.get(layer, "#9CA3AF"),
                }
            })

        edges = []
        for u, v, attrs in self._graph.edges(data=True):
            edges.append({
                "data": {
                    "source":    u,
                    "target":    v,
                    "label":     attrs.get("transform", ""),
                    "transform": attrs.get("transform", "unknown"),
                    "stage":     attrs.get("stage", "unknown"),
                }
            })

        return {"nodes": nodes, "edges": edges}

    def persist_to_stardog(self, stardog_url: str, database: str) -> None:
        """
        Persist the lineage graph to Stardog as RDF triples.
        Delegates to graph/stardog_client.py.

        Args:
            stardog_url: Stardog server URL (e.g. http://localhost:5820)
            database:    Stardog database name.
        """
        try:
            from graph.stardog_client import StardogClient
            client = StardogClient(url=stardog_url, database=database)
            client.write_graph(self)
            logger.info(
                "LineageGraph | persisted to Stardog | db=%s nodes=%d edges=%d",
                database,
                self._graph.number_of_nodes(),
                self._graph.number_of_edges(),
            )
        except ImportError:
            logger.warning(
                "StardogClient not available. "
                "Install stardog-python or check graph/stardog_client.py."
            )
        except Exception as exc:
            logger.error("persist_to_stardog() failed: %s", exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_edge(
        self,
        source: str,
        target: str,
        transform: str,
        stage: str,
        batch_id: str,
        metadata: Dict[str, Any],
    ) -> None:
        """Add a single directed edge to the NetworkX graph."""
        self._graph.add_node(source)
        self._graph.add_node(target)
        self._graph.add_edge(
            source,
            target,
            transform=transform,
            stage=stage,
            batch_id=batch_id,
            metadata=metadata,
        )

    def __repr__(self) -> str:
        return (
            f"LineageGraph("
            f"nodes={self._graph.number_of_nodes()}, "
            f"edges={self._graph.number_of_edges()}, "
            f"batches={len(self._batch_ids)})"
        )