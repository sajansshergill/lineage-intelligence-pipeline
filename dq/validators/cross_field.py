"""
dq/validators/cross_field.py
-----------------------------
Validates relationships between two or more columns in the same row.

Used by rules_engine.py when a rule has type: cross_field.

Supports expression-based rules evaluated as PySpark SQL expressions,
enabling flexible multi-column consistency checks without code changes.

Integrates with:
    dq/rules_engine.py    -> calls CrossFieldValidator.validate()
    dq/violation_logger.py -> receives returned violations for DuckDB write
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq.validators.null_check import RuleViolation

logger = logging.getLogger(__name__)


class CrossFieldValidator:
    """
    Evaluates a SQL-style boolean expression across multiple columns.
    Rows where the expression evaluates to False are flagged as violations.

    Rule config shape (from rules_config.yaml):
        - name: settlement_date_not_before_trade_date
          fields: [trade_date, settlement_date]
          type: cross_field
          expr: "settlement_date >= trade_date"
          severity: WARN
          stage: post_clean
          row_key_col: trade_id

        - name: buy_sell_notional_sign_check
          fields: [direction, notional]
          type: cross_field
          expr: "NOT (direction = 'SELL' AND notional > 0 AND net_notional_usd > 0)"
          severity: ERROR
          stage: post_clean

    The `expr` field is passed directly to PySpark's expr() function,
    so any valid Spark SQL boolean expression is supported.
    """

    # Built-in named expressions for common financial checks
    # Referenced in rules_config.yaml as expr_alias instead of expr
    NAMED_EXPRESSIONS: Dict[str, str] = {
        "settlement_after_trade":
            "settlement_date >= trade_date",
        "notional_matches_direction":
            "NOT (direction = 'SELL' AND notional < 0)",
        "quantity_positive_if_buy":
            "NOT (direction = 'BUY' AND quantity <= 0)",
        "lei_present_if_large_trade":
            "NOT (notional_usd > 10000000 AND (lei_code IS NULL OR lei_code = ''))",
        "settlement_within_10_days":
            "days_to_settlement BETWEEN 0 AND 10",
    }

    def validate(
        self,
        df: DataFrame,
        rule: Dict[str, Any],
        batch_id: str,
    ) -> List[RuleViolation]:
        """
        Evaluate a cross-field expression and return violations.

        Args:
            df:       DataFrame to validate.
            rule:     Rule definition dict from rules_config.yaml.
            batch_id: Current batch identifier.

        Returns:
            List of RuleViolation for rows where the expression is False.
        """
        rule_name   = rule["name"]
        severity    = rule.get("severity", "WARN")
        stage       = rule.get("stage", "unknown")
        row_key_col = rule.get("row_key_col", "trade_id")
        fields      = rule.get("fields", [])

        # Resolve expression — inline expr takes priority over alias
        expr_str = rule.get("expr") or self.NAMED_EXPRESSIONS.get(rule.get("expr_alias", ""))

        if not expr_str:
            logger.warning(
                "CrossField | rule='%s' has no expr or expr_alias — skipping.", rule_name
            )
            return []

        # Validate that referenced fields exist in the DataFrame
        missing_fields = [f for f in fields if f not in df.columns]
        if missing_fields:
            logger.warning(
                "CrossField | rule='%s' references missing columns %s — skipping.",
                rule_name, missing_fields,
            )
            return []

        # Evaluate: rows where the expression is FALSE are violations
        try:
            failing_rows = df.filter(~F.expr(expr_str))
        except Exception as exc:
            logger.error(
                "CrossField | rule='%s' expression '%s' failed to evaluate: %s",
                rule_name, expr_str, exc,
            )
            return []

        fail_count = failing_rows.count()

        if fail_count == 0:
            logger.debug("CrossField | rule='%s' PASSED", rule_name)
            return []

        logger.warning(
            "CrossField | rule='%s' expr='%s' severity=%s failures=%d",
            rule_name, expr_str, severity, fail_count,
        )

        # Build select list for violation capture
        key_col_present = row_key_col in df.columns
        capture_cols = list(fields)
        if key_col_present and row_key_col not in capture_cols:
            capture_cols.append(row_key_col)

        sample = failing_rows.select(*capture_cols).limit(1000).collect()

        violations = []
        for row in sample:
            # Summarize the offending field values as "field=value, field=value"
            offending = ", ".join(
                f"{f}={row[f]}" for f in fields if f in row.__fields__
            )
            violations.append(
                RuleViolation(
                    batch_id=batch_id,
                    rule_name=rule_name,
                    severity=severity,
                    field_name=", ".join(fields),
                    offending_value=offending,
                    row_key=str(row[row_key_col]) if key_col_present else None,
                    stage=stage,
                )
            )

        return violations