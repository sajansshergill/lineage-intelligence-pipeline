"""
dq/validators/null_check.py
---------------------------
Validates that specified columns contain no null values.

Used by rules_engine.py when a rule has type: null_check.
Returns a list of RuleViolation objects — one per offending row.

Integrates with:
    dq/rules_engine.py   -> calls NullCheckValidator.validate()
    dq/violation_logger.py -> receives returned violations for DuckDB write
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared violation dataclass (imported by all validators)
# ---------------------------------------------------------------------------

@dataclass
class RuleViolation:
    """
    A single data quality rule violation.

    Fields:
        batch_id:        Batch that produced the violation.
        rule_name:       Name from rules_config.yaml.
        severity:        WARN | ERROR | CRITICAL.
        field_name:      Column that failed the rule.
        offending_value: String representation of the bad value (nullable).
        row_key:         Value of the row identifier column (e.g. trade_id).
        stage:           Pipeline checkpoint where the rule ran.
        ts:              UTC ISO timestamp of detection.
    """
    batch_id:        str
    rule_name:       str
    severity:        str
    field_name:      str
    offending_value: Optional[str]
    row_key:         Optional[str]
    stage:           str
    ts:              str = field(default_factory=lambda: __import__("datetime").datetime.utcnow().isoformat())


# ---------------------------------------------------------------------------
# NullCheckValidator
# ---------------------------------------------------------------------------

class NullCheckValidator:
    """
    Checks that a specified column contains no null values.

    Rule config shape (from rules_config.yaml):
        - name: notional_not_null
          field: notional
          type: null_check
          severity: CRITICAL
          stage: post_ingest
          row_key_col: trade_id       # optional, defaults to trade_id
    """

    def validate(
        self,
        df: DataFrame,
        rule: Dict[str, Any],
        batch_id: str,
    ) -> List[RuleViolation]:
        """
        Scan the DataFrame for null values in the configured field.

        Args:
            df:       DataFrame to validate (at the current checkpoint).
            rule:     Rule definition dict from rules_config.yaml.
            batch_id: Current batch identifier.

        Returns:
            List of RuleViolation — one per null row found.
            Empty list means the rule passed.
        """
        field_name   = rule["field"]
        severity     = rule.get("severity", "ERROR")
        stage        = rule.get("stage", "unknown")
        rule_name    = rule["name"]
        row_key_col  = rule.get("row_key_col", "trade_id")

        if field_name not in df.columns:
            logger.warning(
                "NullCheck | rule='%s' field='%s' not in DataFrame — skipping.",
                rule_name, field_name,
            )
            return []

        # Filter to rows where the field is null
        null_rows = df.filter(F.col(field_name).isNull())
        null_count = null_rows.count()

        if null_count == 0:
            logger.debug("NullCheck | rule='%s' PASSED (0 nulls)", rule_name)
            return []

        logger.warning(
            "NullCheck | rule='%s' field='%s' severity=%s null_count=%d",
            rule_name, field_name, severity, null_count,
        )

        # Collect row keys for violation records
        # Limit to 1000 to avoid driver OOM on large batches
        key_col_present = row_key_col in df.columns
        sample = (
            null_rows
            .select(row_key_col if key_col_present else F.lit(None).alias("_key"))
            .limit(1000)
            .collect()
        )

        violations = [
            RuleViolation(
                batch_id=batch_id,
                rule_name=rule_name,
                severity=severity,
                field_name=field_name,
                offending_value=None,
                row_key=str(row[0]) if key_col_present else None,
                stage=stage,
            )
            for row in sample
        ]

        return violations