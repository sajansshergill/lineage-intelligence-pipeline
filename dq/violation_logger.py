"""
dq/violation_logger.py
-----------------------
Persists DQ rule violations and batch DQ scores to DuckDB.

Two tables written:
    dq_violations : one row per violation record (field-level detail)
    dq_scores     : one row per checkpoint per batch (aggregate score)

Both tables are queried by:
    dashboard/pages/dq_violations.py  -> violation drilldown UI
    dashboard/pages/run_history.py    -> DQ score trend chart

Integrates with:
    dq/rules_engine.py  -> calls write() and write_score() after each checkpoint
    dq/validators/      -> RuleViolation objects produced by validators
"""

from __future__ import annotations

import logging
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from dq.rules_engine import DQResult
    from dq.validators.null_check import RuleViolation

logger = logging.getLogger(__name__)


class ViolationLogger:
    """
    Writes DQ violation records and checkpoint scores to DuckDB.

    Tables created on first write if they do not exist:

    dq_violations:
        batch_id, rule_name, severity, field_name,
        offending_value, row_key, stage, ts

    dq_scores:
        batch_id, checkpoint, rules_run, rules_passed, rules_failed,
        total_violations, critical_count, error_count, warn_count,
        dq_score, duration_sec, recorded_at
    """

    def __init__(self, duckdb_path: str = "data/pipeline.duckdb"):
        self.duckdb_path = duckdb_path
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(self, violations: List["RuleViolation"]) -> None:
        """
        Persist a list of RuleViolation objects to dq_violations table.

        Args:
            violations: List of RuleViolation dataclass instances.
        """
        if not violations:
            return

        try:
            import duckdb
            con = duckdb.connect(self.duckdb_path)

            rows = [
                (
                    v.batch_id,
                    v.rule_name,
                    v.severity,
                    v.field_name,
                    v.offending_value,
                    v.row_key,
                    v.stage,
                    v.ts,
                )
                for v in violations
            ]

            con.executemany(
                """
                INSERT INTO dq_violations
                    (batch_id, rule_name, severity, field_name,
                     offending_value, row_key, stage, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            con.close()

            logger.info(
                "ViolationLogger | wrote %d violations to DuckDB", len(rows)
            )

        except Exception as exc:
            logger.error("ViolationLogger.write() failed: %s", exc, exc_info=True)

    def write_score(self, result: "DQResult") -> None:
        """
        Persist a DQResult checkpoint summary to dq_scores table.

        Args:
            result: DQResult from rules_engine.run_checkpoint().
        """
        try:
            import duckdb
            from datetime import datetime

            con = duckdb.connect(self.duckdb_path)
            con.execute(
                """
                INSERT INTO dq_scores (
                    batch_id, checkpoint, rules_run, rules_passed, rules_failed,
                    total_violations, critical_count, error_count, warn_count,
                    dq_score, duration_sec, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.batch_id,
                    result.checkpoint,
                    result.rules_run,
                    result.rules_passed,
                    result.rules_failed,
                    result.total_violations,
                    result.critical_count,
                    result.error_count,
                    result.warn_count,
                    result.dq_score,
                    result.duration_sec,
                    datetime.utcnow().isoformat(),
                ),
            )
            con.close()

            logger.info(
                "ViolationLogger | score recorded batch='%s' checkpoint='%s' score=%.1f",
                result.batch_id, result.checkpoint, result.dq_score,
            )

        except Exception as exc:
            logger.error("ViolationLogger.write_score() failed: %s", exc, exc_info=True)

    def query_violations(
        self,
        batch_id: str = None,
        severity: str = None,
        stage: str = None,
        rule_name: str = None,
        limit: int = 500,
    ) -> List[dict]:
        """
        Query violations from DuckDB with optional filters.
        Used by the Streamlit dashboard violation drilldown page.

        Args:
            batch_id:  Filter to a specific batch.
            severity:  Filter to WARN | ERROR | CRITICAL.
            stage:     Filter to a specific checkpoint.
            rule_name: Filter to a specific rule.
            limit:     Max rows to return.

        Returns:
            List of dicts representing violation rows.
        """
        try:
            import duckdb

            conditions = []
            params = []

            if batch_id:
                conditions.append("batch_id = ?")
                params.append(batch_id)
            if severity:
                conditions.append("severity = ?")
                params.append(severity.upper())
            if stage:
                conditions.append("stage = ?")
                params.append(stage)
            if rule_name:
                conditions.append("rule_name = ?")
                params.append(rule_name)

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            sql = f"""
                SELECT batch_id, rule_name, severity, field_name,
                       offending_value, row_key, stage, ts
                FROM dq_violations
                {where_clause}
                ORDER BY ts DESC
                LIMIT {limit}
            """

            con = duckdb.connect(self.duckdb_path, read_only=True)
            rows = con.execute(sql, params).fetchall()
            cols = ["batch_id", "rule_name", "severity", "field_name",
                    "offending_value", "row_key", "stage", "ts"]
            con.close()

            return [dict(zip(cols, row)) for row in rows]

        except Exception as exc:
            logger.error("ViolationLogger.query_violations() failed: %s", exc)
            return []

    def query_scores(self, last_n_batches: int = 20) -> List[dict]:
        """
        Query the most recent DQ scores for the run history dashboard.

        Args:
            last_n_batches: Number of most recent batch scores to return.

        Returns:
            List of dicts with score summary per batch/checkpoint.
        """
        try:
            import duckdb

            con = duckdb.connect(self.duckdb_path, read_only=True)
            rows = con.execute(
                f"""
                SELECT batch_id, checkpoint, dq_score, rules_run,
                       rules_passed, rules_failed, total_violations,
                       critical_count, error_count, warn_count,
                       duration_sec, recorded_at
                FROM dq_scores
                ORDER BY recorded_at DESC
                LIMIT {last_n_batches}
                """
            ).fetchall()
            cols = [
                "batch_id", "checkpoint", "dq_score", "rules_run",
                "rules_passed", "rules_failed", "total_violations",
                "critical_count", "error_count", "warn_count",
                "duration_sec", "recorded_at",
            ]
            con.close()

            return [dict(zip(cols, row)) for row in rows]

        except Exception as exc:
            logger.error("ViolationLogger.query_scores() failed: %s", exc)
            return []

    def violation_summary_by_rule(self, batch_id: str = None) -> List[dict]:
        """
        Aggregate violation counts grouped by rule name and severity.
        Used by the Streamlit DQ scorecard component.

        Args:
            batch_id: Optional filter to a specific batch.

        Returns:
            List of dicts: rule_name, severity, violation_count.
        """
        try:
            import duckdb

            where = f"WHERE batch_id = '{batch_id}'" if batch_id else ""
            con = duckdb.connect(self.duckdb_path, read_only=True)
            rows = con.execute(
                f"""
                SELECT rule_name, severity, COUNT(*) as violation_count
                FROM dq_violations
                {where}
                GROUP BY rule_name, severity
                ORDER BY
                    CASE severity
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'ERROR'    THEN 2
                        WHEN 'WARN'     THEN 3
                        ELSE 4
                    END,
                    violation_count DESC
                """
            ).fetchall()
            con.close()

            return [
                {"rule_name": r[0], "severity": r[1], "violation_count": r[2]}
                for r in rows
            ]

        except Exception as exc:
            logger.error("ViolationLogger.violation_summary_by_rule() failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Create DuckDB tables if they do not already exist."""
        try:
            import duckdb

            con = duckdb.connect(self.duckdb_path)
            con.execute("""
                CREATE TABLE IF NOT EXISTS dq_violations (
                    batch_id        VARCHAR,
                    rule_name       VARCHAR,
                    severity        VARCHAR,
                    field_name      VARCHAR,
                    offending_value VARCHAR,
                    row_key         VARCHAR,
                    stage           VARCHAR,
                    ts              VARCHAR
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS dq_scores (
                    batch_id         VARCHAR,
                    checkpoint       VARCHAR,
                    rules_run        INTEGER,
                    rules_passed     INTEGER,
                    rules_failed     INTEGER,
                    total_violations INTEGER,
                    critical_count   INTEGER,
                    error_count      INTEGER,
                    warn_count       INTEGER,
                    dq_score         DOUBLE,
                    duration_sec     DOUBLE,
                    recorded_at      VARCHAR
                )
            """)
            con.close()
            logger.debug("ViolationLogger | DuckDB tables verified.")

        except Exception as exc:
            logger.warning("ViolationLogger._ensure_tables() failed: %s", exc)