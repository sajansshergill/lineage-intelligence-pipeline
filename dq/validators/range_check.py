"""
dq/validators/range_check.py
-----------------------------
Validates that numeric column values fall within a configured min/max range.

Used by rules_engine.py when a rule has type: range_check.

Integrates with:
    dq/rules_engine.py    -> calls RangeCheckValidator.validate()
    dq/violation_logger.py -> receives returned violations for DuckDB write
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq.validators.null_check import RuleViolation

logger = logging.getLogger(__name__)


class RangeCheckValidator:
    """
    Checks that a numeric column's values fall within [min, max].

    Rule config shape (from rules_config.yaml):
        - name: notional_positive
          field: notional
          type: range_check
          min: 0.01
          max: 1_000_000_000
          severity: ERROR
          stage: post_clean
          row_key_col: trade_id       # optional

    Either min or max can be omitted for a one-sided bound.
    """

    def validate(
        self,
        df: DataFrame,
        rule: Dict[str, Any],
        batch_id: str,
    ) -> List[RuleViolation]:
        """
        Scan the DataFrame for values outside the configured [min, max] range.

        Args:
            df:       DataFrame to validate.
            rule:     Rule definition dict from rules_config.yaml.
            batch_id: Current batch identifier.

        Returns:
            List of RuleViolation — one per out-of-range row (capped at 1000).
            Empty list means the rule passed.
        """
        field_name  = rule["field"]
        severity    = rule.get("severity", "ERROR")
        stage       = rule.get("stage", "unknown")
        rule_name   = rule["name"]
        row_key_col = rule.get("row_key_col", "trade_id")
        min_val     = rule.get("min", None)
        max_val     = rule.get("max", None)

        if field_name not in df.columns:
            logger.warning(
                "RangeCheck | rule='%s' field='%s' not in DataFrame — skipping.",
                rule_name, field_name,
            )
            return []

        if min_val is None and max_val is None:
            logger.warning(
                "RangeCheck | rule='%s' has neither min nor max — skipping.", rule_name
            )
            return []

        # Build filter condition for out-of-range rows
        col = F.col(field_name)
        condition = F.lit(False)

        if min_val is not None:
            condition = condition | (col < F.lit(float(min_val)))
        if max_val is not None:
            condition = condition | (col > F.lit(float(max_val)))

        # Also flag nulls as range violations if they were not caught upstream
        condition = condition | col.isNull()

        failing_rows = df.filter(condition)
        fail_count = failing_rows.count()

        if fail_count == 0:
            logger.debug("RangeCheck | rule='%s' PASSED", rule_name)
            return []

        logger.warning(
            "RangeCheck | rule='%s' field='%s' severity=%s failures=%d "
            "min=%s max=%s",
            rule_name, field_name, severity, fail_count, min_val, max_val,
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
                offending_value=str(row[field_name]) if row[field_name] is not None else "NULL",
                row_key=str(row[row_key_col]) if key_col_present else None,
                stage=stage,
            )
            for row in sample
        ]

        return violations