"""
etl/error_handler.py
--------------------
Dead-letter queue and retry logic for the ETL pipeline.

DeadLetterQueue:
    Accepts failed records (full DataFrame partition) with a failure reason,
    batch ID, and originating stage. Writes to a separate Parquet partition
    under data/dead_letter/ for audit and reprocessing.

RetryHandler:
    Wraps any callable with exponential backoff. Configurable max attempts
    and base delay. Raises the last exception if all retries are exhausted.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)

DLQ_BASE_PATH = "data/dead_letter"


# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------

class DeadLetterQueue:
    """
    Routes failed records to a partitioned Parquet store.

    Schema written alongside the original columns:
        _dlq_batch_id   : batch that produced the failure
        _dlq_stage      : transform/stage name where failure occurred
        _dlq_reason     : exception message or DQ rule name
        _dlq_ts         : UTC timestamp of routing
    """

    DLQ_COLS = ["_dlq_batch_id", "_dlq_stage", "_dlq_reason", "_dlq_ts"]

    def __init__(self, spark: SparkSession, base_path: str = DLQ_BASE_PATH):
        self.spark = spark
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        # In-memory counter: batch_id -> count of dead records
        self._counts: Dict[str, int] = defaultdict(int)

    def write(
        self,
        df: DataFrame,
        reason: str,
        batch_id: str,
        stage: str,
    ) -> None:
        """
        Tag every row in df with DLQ metadata and write to the dead-letter
        partition for this batch.

        Args:
            df:       The DataFrame of failed records (full partition).
            reason:   Human-readable failure reason (exception message or rule name).
            batch_id: Current batch identifier.
            stage:    The transform/stage that triggered the routing.
        """
        count = df.count()
        if count == 0:
            return

        tagged = (
            df
            .withColumn("_dlq_batch_id", F.lit(batch_id))
            .withColumn("_dlq_stage", F.lit(stage))
            .withColumn("_dlq_reason", F.lit(reason[:512]))   # truncate long messages
            .withColumn("_dlq_ts", F.current_timestamp())
        )

        output_path = str(self.base_path / f"batch={batch_id}" / f"stage={stage}")
        (
            tagged
            .coalesce(1)
            .write
            .mode("append")
            .parquet(output_path)
        )

        self._counts[batch_id] += count
        logger.warning(
            "DLQ | batch=%s stage=%s reason='%s' records=%d -> %s",
            batch_id, stage, reason[:80], count, output_path,
        )

    def write_rows(
        self,
        df: DataFrame,
        failed_row_keys: list,
        key_col: str,
        reason: str,
        batch_id: str,
        stage: str,
    ) -> DataFrame:
        """
        Route specific rows (identified by key_col values) to DLQ,
        returning the remaining clean DataFrame.

        Args:
            df:               Full DataFrame.
            failed_row_keys:  List of key values to route to DLQ.
            key_col:          Column name used as the row identifier.
            reason:           Failure reason.
            batch_id:         Current batch ID.
            stage:            Originating stage name.

        Returns:
            DataFrame with failed rows removed.
        """
        if not failed_row_keys:
            return df

        failed_df = df.filter(F.col(key_col).isin(failed_row_keys))
        clean_df = df.filter(~F.col(key_col).isin(failed_row_keys))

        self.write(failed_df, reason=reason, batch_id=batch_id, stage=stage)
        return clean_df

    def count_for_batch(self, batch_id: str) -> int:
        """Return the number of records routed to DLQ for a given batch."""
        return self._counts.get(batch_id, 0)

    def read(self, batch_id: Optional[str] = None) -> Optional[DataFrame]:
        """
        Read dead-letter records back for reprocessing or inspection.

        Args:
            batch_id: If provided, read only records from that batch.
                      If None, read all dead-letter records.

        Returns:
            DataFrame of dead-letter records, or None if none exist.
        """
        path = (
            str(self.base_path / f"batch={batch_id}")
            if batch_id
            else str(self.base_path)
        )
        try:
            return self.spark.read.parquet(path)
        except Exception:
            logger.info("No dead-letter records found at: %s", path)
            return None

    def reprocess(
        self,
        batch_id: str,
        pipeline_run_fn: Callable[[DataFrame], DataFrame],
    ) -> Optional[DataFrame]:
        """
        Re-run failed records through a provided function (typically a
        corrected pipeline stage).

        Args:
            batch_id:        Batch to reprocess.
            pipeline_run_fn: Function that accepts and returns a DataFrame.

        Returns:
            Reprocessed DataFrame, or None if no records to reprocess.
        """
        df = self.read(batch_id=batch_id)
        if df is None:
            logger.info("Nothing to reprocess for batch=%s", batch_id)
            return None

        # Strip DLQ metadata columns before reprocessing
        clean_cols = [c for c in df.columns if c not in self.DLQ_COLS]
        stripped = df.select(clean_cols)

        logger.info(
            "Reprocessing %d dead-letter records from batch=%s",
            stripped.count(), batch_id,
        )
        return pipeline_run_fn(stripped)


# ---------------------------------------------------------------------------
# Retry handler
# ---------------------------------------------------------------------------

class RetryHandler:
    """
    Wraps a callable with exponential backoff retry logic.

    Behaviour:
        - Attempt 1: immediate
        - Attempt 2: sleep backoff_base seconds
        - Attempt 3: sleep backoff_base * 2 seconds
        - ... up to max_attempts
        - If all attempts fail, re-raise the last exception.

    Usage:
        handler = RetryHandler(max_attempts=3, backoff_base_sec=2.0)
        result_df = handler.execute_with_retry(fn=transform.execute, df=df, meta=meta)
    """

    def __init__(self, max_attempts: int = 3, backoff_base: float = 2.0):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base

    def execute_with_retry(self, fn: Callable, *args, **kwargs) -> Any:
        """
        Execute fn(*args, **kwargs) with retry on failure.

        Args:
            fn:      The callable to execute (e.g. transform.execute).
            *args:   Positional arguments forwarded to fn.
            **kwargs: Keyword arguments forwarded to fn.

        Returns:
            The return value of fn on success.

        Raises:
            The last exception raised by fn if all retries are exhausted.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                if attempt > 1:
                    sleep_sec = self.backoff_base * (2 ** (attempt - 2))
                    logger.info(
                        "Retry attempt %d/%d for '%s' — sleeping %.1fs",
                        attempt, self.max_attempts,
                        getattr(fn, "__self__", fn).__class__.__name__,
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)

                return fn(*args, **kwargs)

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Attempt %d/%d failed: %s",
                    attempt, self.max_attempts, exc,
                )

        logger.error(
            "All %d attempts exhausted. Last error: %s",
            self.max_attempts, last_exc,
        )
        raise last_exc

    def with_fallback(
        self,
        fn: Callable,
        fallback_fn: Callable,
        *args,
        **kwargs,
    ) -> Any:
        """
        Try fn with retry; if all attempts fail, call fallback_fn instead.

        Args:
            fn:          Primary callable.
            fallback_fn: Called with the same args if fn fails all retries.

        Returns:
            Result of fn on success, or result of fallback_fn on failure.
        """
        try:
            return self.execute_with_retry(fn, *args, **kwargs)
        except Exception as exc:
            logger.warning(
                "Primary fn failed after %d retries. Calling fallback. Error: %s",
                self.max_attempts, exc,
            )
            return fallback_fn(*args, **kwargs)