"""
etl/transforms/clean.py
-----------------------
Data cleaning transforms: NullFill, TypeCast, Dedup.

Each transform:
  - Registers itself via @register_transform
  - Emits lineage edges for every column it touches
  - Routes unrecoverable rows to the DLQ
  - Is independently testable with a local SparkSession fixture
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, TimestampType,
)

from etl.framework import BaseTransform, BatchMeta
from etl.transforms.registry import register_transform

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NullFill
# ---------------------------------------------------------------------------

@register_transform("NullFill")
class NullFill(BaseTransform):
    """
    Fill null values in specified columns with configured defaults.

    Config (from pipeline_config.yaml):
        null_fill:
          notional: 0.0
          currency: "USD"
          direction: "UNKNOWN"
          settlement_status: "PENDING"

    Lineage emitted:
        raw.<col> -> clean.<col> [transform=null_fill]
    """

    name = "NullFill"
    version = "1.0.0"
    stage = "clean"

    # Sensible defaults if not overridden in config
    DEFAULT_FILLS: Dict[str, Any] = {
        "notional":          0.0,
        "quantity":          0.0,
        "currency":          "USD",
        "direction":         "UNKNOWN",
        "settlement_status": "PENDING",
        "asset_class":       "UNKNOWN",
        "exchange":          "UNKNOWN",
    }

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        fills = {**self.DEFAULT_FILLS, **self.config.get("null_fill", {})}

        for col_name, fill_value in fills.items():
            if col_name not in df.columns:
                continue

            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count == 0:
                continue

            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(fill_value))
                 .otherwise(F.col(col_name))
            )

            # Emit lineage edge
            if self.tracker:
                self.tracker.emit(
                    source=f"raw.{col_name}",
                    target=f"clean.{col_name}",
                    transform="null_fill",
                    stage=self.stage,
                    metadata={"null_count": null_count, "fill_value": str(fill_value)},
                )

            logger.debug(
                "NullFill | col='%s' filled=%d value=%s",
                col_name, null_count, fill_value,
            )

        return df

    def validate_schema(self, df: DataFrame) -> List[str]:
        errors = []
        required = ["trade_id"]
        for col in required:
            if col not in df.columns:
                errors.append(f"NullFill requires column '{col}' — not found in schema.")
        return errors


# ---------------------------------------------------------------------------
# TypeCast
# ---------------------------------------------------------------------------

@register_transform("TypeCast")
class TypeCast(BaseTransform):
    """
    Cast columns to their canonical data types for the star schema.

    Type mapping (applied in order):
        notional, quantity  -> DoubleType
        trade_id            -> StringType
        trade_date          -> TimestampType  (parsed from ISO string)
        settlement_date     -> TimestampType
        quantity            -> LongType (integer lots)

    Rows where the cast fails (e.g. "abc" in notional) are routed to DLQ.

    Lineage emitted:
        raw.<col> -> typed.<col> [transform=type_cast]
    """

    name = "TypeCast"
    version = "1.0.0"
    stage = "clean"

    CAST_MAP = {
        "notional":         DoubleType(),
        "quantity":         DoubleType(),
        "trade_id":         StringType(),
        "trade_date":       TimestampType(),
        "settlement_date":  TimestampType(),
        "counterparty_id":  StringType(),
        "product_id":       StringType(),
        "currency":         StringType(),
        "direction":        StringType(),
    }

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        cast_map = {**self.CAST_MAP, **self.config.get("type_cast", {})}

        # Tag each row with a row hash before casting — used to identify
        # cast failures when we compare row counts.
        df = df.withColumn("_row_hash", F.md5(F.concat_ws("|", *df.columns)))

        failed_hashes = []

        for col_name, target_type in cast_map.items():
            if col_name not in df.columns:
                continue

            # Attempt cast; Spark returns null on failure
            cast_col = F.col(col_name).cast(target_type)

            # Detect rows where cast produced null but original was non-null
            newly_null = df.filter(
                F.col(col_name).isNotNull() & cast_col.isNull()
            ).select("_row_hash").rdd.flatMap(lambda r: r).collect()

            if newly_null:
                failed_hashes.extend(newly_null)
                logger.warning(
                    "TypeCast | col='%s' cast_failures=%d -> DLQ",
                    col_name, len(newly_null),
                )

            df = df.withColumn(col_name, cast_col)

            if self.tracker:
                self.tracker.emit(
                    source=f"raw.{col_name}",
                    target=f"typed.{col_name}",
                    transform="type_cast",
                    stage=self.stage,
                    metadata={"target_type": str(target_type)},
                )

        # Route cast-failed rows to DLQ
        if failed_hashes and self.dlq:
            df = self.dlq.write_rows(
                df,
                failed_row_keys=failed_hashes,
                key_col="_row_hash",
                reason="type_cast_failure",
                batch_id=meta.batch_id,
                stage=self.name,
            )

        # Drop helper column
        df = df.drop("_row_hash")
        return df


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

@register_transform("Dedup")
class Dedup(BaseTransform):
    """
    Remove duplicate records based on the natural key (trade_id by default).

    When duplicates exist, keep the row with the latest trade_date.
    Duplicate rows are written to DLQ with reason='duplicate_trade_id'.

    Config:
        dedup:
          key_cols: [trade_id]          # columns that define uniqueness
          order_col: trade_date         # tiebreaker — keep latest
          order_asc: false              # descending = latest first

    Lineage emitted:
        typed.trade_id -> dedup.trade_id [transform=dedup]
    """

    name = "Dedup"
    version = "1.0.0"
    stage = "clean"

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        dedup_cfg = self.config.get("dedup", {})
        key_cols: List[str] = dedup_cfg.get("key_cols", ["trade_id"])
        order_col: str = dedup_cfg.get("order_col", "trade_date")
        order_asc: bool = dedup_cfg.get("order_asc", False)

        # Validate columns exist
        missing = [c for c in key_cols + [order_col] if c not in df.columns]
        if missing:
            logger.warning("Dedup: columns not found, skipping: %s", missing)
            return df

        from pyspark.sql.window import Window

        original_count = df.count()

        sort_dir = F.asc(order_col) if order_asc else F.desc(order_col)
        window = Window.partitionBy(*key_cols).orderBy(sort_dir)

        df = df.withColumn("_dedup_rank", F.row_number().over(window))

        # Send duplicates (rank > 1) to DLQ
        duplicates = df.filter(F.col("_dedup_rank") > 1)
        dup_count = duplicates.count()

        if dup_count > 0 and self.dlq:
            self.dlq.write(
                duplicates.drop("_dedup_rank"),
                reason="duplicate_trade_id",
                batch_id=meta.batch_id,
                stage=self.name,
            )
            logger.info("Dedup | removed=%d duplicates -> DLQ", dup_count)

        clean_df = df.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")

        if self.tracker:
            for col in key_cols:
                self.tracker.emit(
                    source=f"typed.{col}",
                    target=f"dedup.{col}",
                    transform="dedup",
                    stage=self.stage,
                    metadata={
                        "key_cols": key_cols,
                        "duplicates_removed": dup_count,
                        "records_before": original_count,
                        "records_after": clean_df.count(),
                    },
                )

        return clean_df

    def validate_schema(self, df: DataFrame) -> List[str]:
        key_cols = self.config.get("dedup", {}).get("key_cols", ["trade_id"])
        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            return [f"Dedup key columns not found in schema: {missing}"]
        return []