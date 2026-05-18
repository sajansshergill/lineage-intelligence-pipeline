"""
dq/validators/referential_integrity.py
---------------------------------------
Validates that column values exist in a reference lookup set.

Used by rules_engine.py when a rule has type: referential_integrity.

Supports two lookup modes:
    1. static_values : list of allowed values defined directly in the rule
    2. duckdb_table  : query a DuckDB table for the allowed value set

Integrates with:
    dq/rules_engine.py    -> calls ReferentialIntegrityValidator.validate()
    dq/violation_logger.py -> receives returned violations for DuckDB write
    models/star_schema.sql -> DimProduct, DimCounterparty used as lookup sources
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq.validators.null_check import RuleViolation

logger = logging.getLogger(__name__)


class ReferentialIntegrityValidator:
    """
    Checks that every non-null value in a field exists within an allowed set.

    Rule config shapes:

    Mode 1 — static values list:
        - name: currency_valid
          field: currency
          type: referential_integrity
          mode: static_values
          allowed_values: [USD, EUR, GBP, JPY, CHF, CAD, AUD, HKD, SGD, CNY]
          severity: ERROR
          stage: post_ingest

    Mode 2 — DuckDB table lookup:
        - name: product_id_valid
          field: product_id
          type: referential_integrity
          mode: duckdb_table
          duckdb_path: data/pipeline.duckdb
          lookup_table: dim_product
          lookup_field: product_id
          severity: ERROR
          stage: post_clean
    """

    def validate(
        self,
        df: DataFrame,
        rule: Dict[str, Any],
        batch_id: str,
    ) -> List[RuleViolation]:
        """
        Check referential integrity for the configured field.

        Args:
            df:       DataFrame to validate.
            rule:     Rule definition dict from rules_config.yaml.
            batch_id: Current batch identifier.

        Returns:
            List of RuleViolation for values not found in the reference set.
        """
        field_name  = rule["field"]
        severity    = rule.get("severity", "ERROR")
        stage       = rule.get("stage", "unknown")
        rule_name   = rule["name"]
        row_key_col = rule.get("row_key_col", "trade_id")
        mode        = rule.get("mode", "static_values")

        if field_name not in df.columns:
            logger.warning(
                "RefIntegrity | rule='%s' field='%s' not in DataFrame — skipping.",
                rule_name, field_name,
            )
            return []

        # Resolve the allowed value set based on mode
        allowed: Set[str] = set()

        if mode == "static_values":
            allowed = {str(v).upper() for v in rule.get("allowed_values", [])}
            if not allowed:
                logger.warning(
                    "RefIntegrity | rule='%s' mode=static_values but allowed_values is empty.",
                    rule_name,
                )
                return []

        elif mode == "duckdb_table":
            allowed = self._load_from_duckdb(rule)
            if not allowed:
                logger.warning(
                    "RefIntegrity | rule='%s' DuckDB lookup returned empty set — skipping.",
                    rule_name,
                )
                return []

        else:
            logger.warning(
                "RefIntegrity | rule='%s' unknown mode='%s' — skipping.", rule_name, mode
            )
            return []

        # Filter rows where field value is not in allowed set (ignore nulls — handled by null_check)
        broadcast_allowed = F.array(*[F.lit(v) for v in allowed])

        failing_rows = df.filter(
            F.col(field_name).isNotNull() &
            ~F.upper(F.col(field_name)).isin(list(allowed))
        )

        fail_count = failing_rows.count()

        if fail_count == 0:
            logger.debug("RefIntegrity | rule='%s' PASSED", rule_name)
            return []

        logger.warning(
            "RefIntegrity | rule='%s' field='%s' severity=%s failures=%d",
            rule_name, field_name, severity, fail_count,
        )

        key_col_present = row_key_col in df.columns
        select_cols = [field_name]
        if key_col_present:
            select_cols.append(row_key_col)

        sample = failing_rows.select(*select_cols).limit(1000).collect()

        violations = [
            RuleViolation(
                batch_id=batch_id,
                rule_name=rule_name,
                severity=severity,
                field_name=field_name,
                offending_value=str(row[field_name]),
                row_key=str(row[row_key_col]) if key_col_present else None,
                stage=stage,
            )
            for row in sample
        ]

        return violations

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_from_duckdb(self, rule: Dict[str, Any]) -> Set[str]:
        """
        Load allowed values from a DuckDB table column.

        Args:
            rule: Rule dict with duckdb_path, lookup_table, lookup_field.

        Returns:
            Set of uppercase string values from the lookup column.
        """
        duckdb_path  = rule.get("duckdb_path", "data/pipeline.duckdb")
        lookup_table = rule.get("lookup_table")
        lookup_field = rule.get("lookup_field")

        if not lookup_table or not lookup_field:
            logger.warning(
                "RefIntegrity | duckdb_table mode requires lookup_table and lookup_field."
            )
            return set()

        try:
            import duckdb
            con = duckdb.connect(duckdb_path, read_only=True)
            rows = con.execute(
                f"SELECT DISTINCT UPPER(CAST({lookup_field} AS VARCHAR)) "
                f"FROM {lookup_table} WHERE {lookup_field} IS NOT NULL"
            ).fetchall()
            con.close()
            result = {row[0] for row in rows}
            logger.debug(
                "RefIntegrity | loaded %d values from %s.%s",
                len(result), lookup_table, lookup_field,
            )
            return result
        except Exception as exc:
            logger.error(
                "RefIntegrity | DuckDB lookup failed for rule '%s': %s",
                rule.get("name"), exc,
            )
            return set()