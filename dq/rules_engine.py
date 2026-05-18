"""
dq/rules_engine.py
------------------
Orchestrates all data quality rule execution for a pipeline batch.

Responsibilities:
    1. Loads rule definitions from dq/rules_config.yaml
    2. Routes each rule to the correct validator (null_check, range_check,
       referential_integrity, cross_field)
    3. Runs rules at configured pipeline checkpoints (post_ingest, post_clean, pre_load)
    4. Aggregates violations and computes a DQ score (0-100) per batch
    5. Writes all violations to DuckDB via violation_logger.py
    6. Returns a DQResult summary for the Streamlit dashboard

Integrates with:
    etl/framework.py       -> Pipeline calls engine.run_checkpoint() after each stage
    dq/rules_config.yaml   -> Rule definitions loaded at init
    dq/violation_logger.py -> Violations persisted after each checkpoint
    dq/validators/         -> Individual validator classes
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pyspark.sql import DataFrame

from dq.validators.null_check import NullCheckValidator, RuleViolation
from dq.validators.range_check import RangeCheckValidator
from dq.validators.referential_integrity import ReferentialIntegrityValidator
from dq.validators.cross_field import CrossFieldValidator
from dq.violation_logger import ViolationLogger

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DQ result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DQResult:
    """
    Summary of a single DQ checkpoint run.

    Fields:
        batch_id:         Batch identifier.
        checkpoint:       Pipeline checkpoint name (post_ingest, post_clean, pre_load).
        rules_run:        Total number of rules evaluated.
        rules_passed:     Rules with zero violations.
        rules_failed:     Rules with one or more violations.
        total_violations: Total violation records across all rules.
        critical_count:   Count of CRITICAL severity violations.
        error_count:      Count of ERROR severity violations.
        warn_count:       Count of WARN severity violations.
        dq_score:         0-100 score (100 = perfect, 0 = all rules failed).
        duration_sec:     Time taken to run all rules at this checkpoint.
        violations:       Full list of RuleViolation objects.
    """
    batch_id:         str
    checkpoint:       str
    rules_run:        int = 0
    rules_passed:     int = 0
    rules_failed:     int = 0
    total_violations: int = 0
    critical_count:   int = 0
    error_count:      int = 0
    warn_count:       int = 0
    dq_score:         float = 100.0
    duration_sec:     float = 0.0
    violations:       List[RuleViolation] = field(default_factory=list)

    def compute_score(self) -> float:
        """
        DQ Score formula:
            score = 100 * (rules_passed / rules_run)
            Adjusted down by severity:
                CRITICAL violations apply a 2x penalty weight
                ERROR violations apply a 1.5x penalty weight
                WARN violations apply a 1x penalty weight
        """
        if self.rules_run == 0:
            return 100.0

        weighted_failures = (
            self.critical_count * 2.0 +
            self.error_count    * 1.5 +
            self.warn_count     * 1.0
        )
        max_weighted = self.rules_run * 2.0  # max possible penalty (all CRITICAL)
        score = max(0.0, 100.0 * (1 - weighted_failures / max_weighted))
        return round(score, 2)


# ---------------------------------------------------------------------------
# DQ Rules engine
# ---------------------------------------------------------------------------

class DQRulesEngine:
    """
    Loads rules from rules_config.yaml and executes them against DataFrames
    at configured pipeline checkpoints.

    Usage:
        engine = DQRulesEngine(config_path="dq/rules_config.yaml",
                               duckdb_path="data/pipeline.duckdb")

        # After the ingest stage:
        result = engine.run_checkpoint(df, checkpoint="post_ingest", batch_id=meta.batch_id)

        # After the clean stage:
        result = engine.run_checkpoint(df, checkpoint="post_clean", batch_id=meta.batch_id)
    """

    VALIDATOR_MAP = {
        "null_check":             NullCheckValidator,
        "range_check":            RangeCheckValidator,
        "referential_integrity":  ReferentialIntegrityValidator,
        "cross_field":            CrossFieldValidator,
    }

    def __init__(
        self,
        config_path: str = "dq/rules_config.yaml",
        duckdb_path: str = "data/pipeline.duckdb",
    ):
        self.config_path = Path(config_path)
        self.duckdb_path = duckdb_path
        self.rules: List[Dict[str, Any]] = self._load_rules()
        self.logger = ViolationLogger(duckdb_path=duckdb_path)

        logger.info(
            "DQRulesEngine initialized | rules=%d | config=%s",
            len(self.rules), config_path,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_checkpoint(
        self,
        df: DataFrame,
        checkpoint: str,
        batch_id: str,
    ) -> DQResult:
        """
        Run all rules configured for a given checkpoint against the DataFrame.

        Args:
            df:          DataFrame at the current pipeline stage.
            checkpoint:  Name matching the 'stage' field in rules_config.yaml
                         (e.g. post_ingest, post_clean, pre_load).
            batch_id:    Current batch identifier.

        Returns:
            DQResult with scores, counts, and full violation list.
        """
        checkpoint_rules = [r for r in self.rules if r.get("stage") == checkpoint]

        if not checkpoint_rules:
            logger.debug("DQ | No rules configured for checkpoint '%s'", checkpoint)
            return DQResult(batch_id=batch_id, checkpoint=checkpoint)

        logger.info(
            "DQ | Running %d rules at checkpoint='%s' batch='%s'",
            len(checkpoint_rules), checkpoint, batch_id,
        )

        t0 = time.perf_counter()
        result = DQResult(batch_id=batch_id, checkpoint=checkpoint)
        all_violations: List[RuleViolation] = []

        for rule in checkpoint_rules:
            rule_violations = self._run_rule(df, rule, batch_id)
            result.rules_run += 1

            if rule_violations:
                result.rules_failed += 1
                all_violations.extend(rule_violations)
                for v in rule_violations:
                    if v.severity == "CRITICAL":
                        result.critical_count += 1
                    elif v.severity == "ERROR":
                        result.error_count += 1
                    elif v.severity == "WARN":
                        result.warn_count += 1
            else:
                result.rules_passed += 1

        result.total_violations = len(all_violations)
        result.violations = all_violations
        result.duration_sec = time.perf_counter() - t0
        result.dq_score = result.compute_score()

        # Persist violations to DuckDB
        if all_violations:
            self.logger.write(all_violations)

        # Persist DQ score for this checkpoint to DuckDB
        self.logger.write_score(result)

        logger.info(
            "DQ | checkpoint='%s' score=%.1f rules=%d passed=%d failed=%d "
            "violations=%d (CRIT=%d ERR=%d WARN=%d) duration=%.2fs",
            checkpoint, result.dq_score, result.rules_run,
            result.rules_passed, result.rules_failed,
            result.total_violations, result.critical_count,
            result.error_count, result.warn_count, result.duration_sec,
        )

        return result

    def run_all(
        self,
        df: DataFrame,
        batch_id: str,
    ) -> Dict[str, DQResult]:
        """
        Run rules at all checkpoints sequentially.
        Useful for standalone DQ profiling outside the pipeline.

        Returns:
            Dict mapping checkpoint name -> DQResult.
        """
        checkpoints = list({r.get("stage", "unknown") for r in self.rules})
        return {
            cp: self.run_checkpoint(df, checkpoint=cp, batch_id=batch_id)
            for cp in sorted(checkpoints)
        }

    def get_rules_for_checkpoint(self, checkpoint: str) -> List[Dict[str, Any]]:
        """Return all rule definitions for a given checkpoint."""
        return [r for r in self.rules if r.get("stage") == checkpoint]

    def list_rules(self) -> List[str]:
        """Return all rule names currently loaded."""
        return [r["name"] for r in self.rules]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_rules(self) -> List[Dict[str, Any]]:
        """Load and validate rule definitions from rules_config.yaml."""
        if not self.config_path.exists():
            logger.warning("DQ rules config not found at: %s", self.config_path)
            return []

        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        rules = config.get("rules", [])

        # Basic validation
        for rule in rules:
            if "name" not in rule:
                raise ValueError(f"DQ rule missing 'name' field: {rule}")
            if "type" not in rule:
                raise ValueError(f"DQ rule '{rule['name']}' missing 'type' field.")
            if rule["type"] not in self.VALIDATOR_MAP:
                raise ValueError(
                    f"DQ rule '{rule['name']}' has unknown type '{rule['type']}'. "
                    f"Supported: {list(self.VALIDATOR_MAP.keys())}"
                )

        logger.info("Loaded %d DQ rules from %s", len(rules), self.config_path)
        return rules

    def _run_rule(
        self,
        df: DataFrame,
        rule: Dict[str, Any],
        batch_id: str,
    ) -> List[RuleViolation]:
        """
        Instantiate the correct validator and run it for a single rule.

        Args:
            df:       DataFrame to validate.
            rule:     Rule definition dict.
            batch_id: Current batch identifier.

        Returns:
            List of RuleViolation objects (empty = passed).
        """
        rule_type = rule["type"]
        validator_cls = self.VALIDATOR_MAP[rule_type]
        validator = validator_cls()

        try:
            violations = validator.validate(df=df, rule=rule, batch_id=batch_id)
            return violations
        except Exception as exc:
            logger.error(
                "DQ | Rule '%s' (type=%s) raised an exception: %s",
                rule["name"], rule_type, exc, exc_info=True,
            )
            # Return a synthetic violation to flag the rule execution failure
            return [
                RuleViolation(
                    batch_id=batch_id,
                    rule_name=rule["name"],
                    severity="ERROR",
                    field_name=rule.get("field", rule.get("fields", ["unknown"])[0]),
                    offending_value=f"RULE_EXECUTION_ERROR: {str(exc)[:200]}",
                    row_key=None,
                    stage=rule.get("stage", "unknown"),
                )
            ]


# ---------------------------------------------------------------------------
# CLI entry point — run DQ checks standalone against a Parquet/CSV file
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run DQ rules against a data file")
    parser.add_argument("--input",      required=True,  help="Path to input CSV or Parquet")
    parser.add_argument("--checkpoint", default=None,   help="Run only this checkpoint")
    parser.add_argument("--batch",      default="manual_run", help="Batch ID override")
    parser.add_argument("--config",     default="dq/rules_config.yaml")
    parser.add_argument("--duckdb",     default="data/pipeline.duckdb")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("DQStandalone").getOrCreate()

    p = Path(args.input)
    if p.suffix == ".csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(p))
    else:
        df = spark.read.parquet(str(p))

    engine = DQRulesEngine(config_path=args.config, duckdb_path=args.duckdb)

    if args.checkpoint:
        result = engine.run_checkpoint(df, checkpoint=args.checkpoint, batch_id=args.batch)
        results = {args.checkpoint: result}
    else:
        results = engine.run_all(df, batch_id=args.batch)

    print("\n=== DQ Results ===")
    for cp, r in results.items():
        print(f"  {cp:20s} | score={r.dq_score:6.1f} | "
              f"rules={r.rules_run} passed={r.rules_passed} failed={r.rules_failed} | "
              f"violations={r.total_violations} "
              f"(CRIT={r.critical_count} ERR={r.error_count} WARN={r.warn_count})")