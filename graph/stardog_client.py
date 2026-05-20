"""
graph/stardog_client.py
-----------------------
Optional Stardog graph database client for RDF/SPARQL lineage persistence.

Stardog is listed as a desired skill in the Morgan Stanley JD.
This client persists the lineage graph as RDF triples using the
financial lineage ontology defined below, making edges queryable
via SPARQL — the query language of graph databases.

Ontology prefix: fin:
    fin:LineageEdge   -> RDF class for a lineage edge
    fin:source        -> source column URI
    fin:target        -> target column URI
    fin:transform     -> transform name string
    fin:stage         -> pipeline stage string
    fin:batchId       -> batch identifier string
    fin:column        -> column name (local part)
    fin:layer         -> pipeline layer (raw, clean, enriched, etc.)

Requires Stardog Community Edition running locally:
    docker run -p 5820:5820 stardog/stardog

Integrates with:
    graph/lineage_graph.py -> calls persist_to_stardog() -> StardogClient.write_graph()
    graph/queries.py       -> calls StardogClient.sparql() for SPARQL queries
    scripts/seed_stardog.sh -> bootstraps Stardog database and schema
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from graph.lineage_graph import LineageGraph

logger = logging.getLogger(__name__)

# RDF namespace for financial lineage ontology
FIN_NS   = "http://morganstanley.com/ontology/fin#"
COL_NS   = "http://morganstanley.com/data/columns/"
EDGE_NS  = "http://morganstanley.com/data/edges/"


class StardogClient:
    """
    Writes and queries the lineage graph in Stardog via HTTP API.

    Usage:
        client = StardogClient(url="http://localhost:5820", database="lineage")
        client.write_graph(lineage_graph)
        results = client.sparql(\"\"\"
            SELECT ?source ?transform WHERE {
                ?edge fin:target <http://morganstanley.com/data/columns/risk_report.eur_exposure> ;
                      fin:source ?source ;
                      fin:transform ?transform .
            }
        \"\"\")
    """

    def __init__(
        self,
        url: str = "http://localhost:5820",
        database: str = "lineage",
        username: str = "admin",
        password: str = "admin",
    ):
        self.url      = url.rstrip("/")
        self.database = database
        self.username = username
        self.password = password
        self.auth     = (username, password)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def write_graph(self, graph: "LineageGraph") -> None:
        """
        Persist all edges from a LineageGraph as RDF triples in Stardog.

        Each lineage edge becomes a named RDF resource with:
            fin:source, fin:target, fin:transform, fin:stage, fin:batchId

        Args:
            graph: Populated LineageGraph instance.
        """
        turtle = self._graph_to_turtle(graph)
        self._post_turtle(turtle)
        logger.info(
            "StardogClient | wrote graph to db='%s' | "
            "nodes=%d edges=%d",
            self.database,
            graph._graph.number_of_nodes(),
            graph._graph.number_of_edges(),
        )

    def write_edges(self, edges: List[Dict[str, Any]]) -> None:
        """
        Write a list of edge dicts (from LineageTracker.to_edge_list()) to Stardog.

        Args:
            edges: List of dicts with keys: source, target, transform, stage, batch_id.
        """
        turtle_lines = [self._prefixes()]
        for i, edge in enumerate(edges):
            edge_uri = f"<{EDGE_NS}edge_{i}>"
            src_uri  = f"<{COL_NS}{self._safe_uri(edge['source'])}>"
            tgt_uri  = f"<{COL_NS}{self._safe_uri(edge['target'])}>"
            turtle_lines.append(
                f"{edge_uri} a fin:LineageEdge ;\n"
                f"    fin:source {src_uri} ;\n"
                f"    fin:target {tgt_uri} ;\n"
                f'    fin:transform "{edge.get("transform", "unknown")}" ;\n'
                f'    fin:stage "{edge.get("stage", "unknown")}" ;\n'
                f'    fin:batchId "{edge.get("batch_id", "unknown")}" .\n'
            )
        self._post_turtle("\n".join(turtle_lines))

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    def sparql(self, query: str) -> List[Dict[str, str]]:
        """
        Execute a SPARQL SELECT query against the lineage database.

        Args:
            query: SPARQL SELECT query string.

        Returns:
            List of dicts mapping variable name -> value string.

        Example:
            results = client.sparql('''
                PREFIX fin: <http://morganstanley.com/ontology/fin#>
                SELECT ?source ?transform WHERE {
                    ?edge fin:target
                        <http://morganstanley.com/data/columns/risk_report.eur_exposure> ;
                          fin:source ?source ;
                          fin:transform ?transform .
                }
            ''')
        """
        try:
            import requests

            response = requests.get(
                f"{self.url}/{self.database}/query",
                params={"query": query},
                headers={"Accept": "application/sparql-results+json"},
                auth=self.auth,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            bindings = data.get("results", {}).get("bindings", [])
            vars_    = data.get("head", {}).get("vars", [])

            return [
                {
                    var: binding.get(var, {}).get("value", "")
                    for var in vars_
                }
                for binding in bindings
            ]

        except Exception as exc:
            logger.error("StardogClient.sparql() failed: %s", exc)
            return []

    def trace_ancestry(self, target_col: str) -> List[Dict[str, str]]:
        """
        SPARQL query to find all upstream sources of a target column.

        Args:
            target_col: Fully-qualified column name (e.g. "risk_report.eur_exposure")

        Returns:
            List of dicts with 'source' and 'transform' keys.
        """
        target_uri = f"{COL_NS}{self._safe_uri(target_col)}"
        query = f"""
            PREFIX fin: <{FIN_NS}>
            SELECT ?source ?transform ?stage WHERE {{
                ?edge fin:target <{target_uri}> ;
                      fin:source ?source ;
                      fin:transform ?transform ;
                      fin:stage ?stage .
            }}
        """
        return self.sparql(query)

    def trace_impact(self, source_col: str) -> List[Dict[str, str]]:
        """
        SPARQL query to find all downstream columns impacted by a source column.

        Args:
            source_col: Fully-qualified column name (e.g. "raw.notional")

        Returns:
            List of dicts with 'target' and 'transform' keys.
        """
        source_uri = f"{COL_NS}{self._safe_uri(source_col)}"
        query = f"""
            PREFIX fin: <{FIN_NS}>
            SELECT ?target ?transform ?stage WHERE {{
                ?edge fin:source <{source_uri}> ;
                      fin:target ?target ;
                      fin:transform ?transform ;
                      fin:stage ?stage .
            }}
        """
        return self.sparql(query)

    def transforms_for_column(self, col: str) -> List[str]:
        """
        SPARQL query to find all transforms that touch a given column
        (either as source or target).

        Args:
            col: Fully-qualified column name.

        Returns:
            Deduplicated list of transform name strings.
        """
        col_uri = f"{COL_NS}{self._safe_uri(col)}"
        query = f"""
            PREFIX fin: <{FIN_NS}>
            SELECT DISTINCT ?transform WHERE {{
                {{
                    ?edge fin:source <{col_uri}> ;
                          fin:transform ?transform .
                }} UNION {{
                    ?edge fin:target <{col_uri}> ;
                          fin:transform ?transform .
                }}
            }}
        """
        results = self.sparql(query)
        return [r["transform"] for r in results if "transform" in r]

    # ------------------------------------------------------------------
    # Database management
    # ------------------------------------------------------------------

    def create_database(self) -> bool:
        """
        Create the Stardog database if it doesn't already exist.

        Returns:
            True if created or already exists, False on error.
        """
        try:
            import requests

            response = requests.post(
                f"{self.url}/admin/databases",
                json={"dbname": self.database},
                auth=self.auth,
                timeout=10,
            )
            if response.status_code in (200, 201):
                logger.info("Stardog database '%s' created.", self.database)
                return True
            elif response.status_code == 409:
                logger.info("Stardog database '%s' already exists.", self.database)
                return True
            else:
                logger.error(
                    "Failed to create Stardog database: %d %s",
                    response.status_code, response.text,
                )
                return False
        except Exception as exc:
            logger.error("create_database() failed: %s", exc)
            return False

    def clear_database(self) -> None:
        """Remove all triples from the database (use with caution)."""
        self.sparql_update("DELETE WHERE { ?s ?p ?o }")
        logger.warning("Stardog database '%s' cleared.", self.database)

    def sparql_update(self, update_query: str) -> bool:
        """
        Execute a SPARQL UPDATE query (INSERT, DELETE, etc.).

        Args:
            update_query: SPARQL UPDATE string.

        Returns:
            True on success, False on failure.
        """
        try:
            import requests

            response = requests.post(
                f"{self.url}/{self.database}/update",
                data={"update": update_query},
                auth=self.auth,
                timeout=30,
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            logger.error("sparql_update() failed: %s", exc)
            return False

    def ping(self) -> bool:
        """Check if Stardog server is reachable."""
        try:
            import requests
            response = requests.get(f"{self.url}/admin/alive", auth=self.auth, timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prefixes(self) -> str:
        return (
            f"@prefix fin: <{FIN_NS}> .\n"
            f"@prefix col: <{COL_NS}> .\n"
            f"@prefix edge: <{EDGE_NS}> .\n\n"
        )

    def _graph_to_turtle(self, graph: "LineageGraph") -> str:
        """Convert a LineageGraph to Turtle RDF format."""
        lines = [self._prefixes()]
        for i, (u, v, attrs) in enumerate(graph._graph.edges(data=True)):
            edge_uri = f"<{EDGE_NS}edge_{i}>"
            src_uri  = f"<{COL_NS}{self._safe_uri(u)}>"
            tgt_uri  = f"<{COL_NS}{self._safe_uri(v)}>"
            lines.append(
                f"{edge_uri} a fin:LineageEdge ;\n"
                f"    fin:source {src_uri} ;\n"
                f"    fin:target {tgt_uri} ;\n"
                f'    fin:transform "{attrs.get("transform", "unknown")}" ;\n'
                f'    fin:stage "{attrs.get("stage", "unknown")}" ;\n'
                f'    fin:batchId "{attrs.get("batch_id", "unknown")}" .\n'
            )
            # Column nodes with layer metadata
            for uri, col_name in [(src_uri, u), (tgt_uri, v)]:
                layer = col_name.split(".")[0] if "." in col_name else "unknown"
                col   = col_name.split(".")[-1]
                lines.append(
                    f"{uri} fin:layer \"{layer}\" ;\n"
                    f'    fin:column "{col}" .\n'
                )
        return "\n".join(lines)

    def _post_turtle(self, turtle: str) -> None:
        """POST Turtle RDF content to the Stardog database."""
        try:
            import requests

            response = requests.post(
                f"{self.url}/{self.database}/add",
                data=turtle.encode("utf-8"),
                headers={"Content-Type": "text/turtle"},
                auth=self.auth,
                timeout=60,
            )
            response.raise_for_status()
        except Exception as exc:
            logger.error("StardogClient._post_turtle() failed: %s", exc)
            raise

    @staticmethod
    def _safe_uri(col_name: str) -> str:
        """Convert a column name to a URI-safe string."""
        return col_name.replace(" ", "_").replace("/", "_")