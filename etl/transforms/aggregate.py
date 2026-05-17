"""
etl/transforms/aggregate.py
---------------------------
Aggregation transforms: ExposureRollup, PositionNet.

These run after enrichment to produce the analytical columns that appear
in the star schema FactTrades and downstream risk reports.
"""

from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from etl.framework import BaseTransform, BatchMeta
from etl.transforms.registry import register_transform

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ExposureRollup
# ---------------------------------------------------------------------------

@register_transform("ExposureRollup")
class ExposureRollup(BaseTransform):
    """
    Compute running and total exposure per counterparty and asset class.

    Adds columns:
        cpty_total_notional_usd   : total USD notional per counterparty
        cpty_trade_count          : number of trades per counterparty
        asset_class_exposure_usd  : total USD notional per asset class
        running_notional_usd      : running sum ordered by trade_date (window)

    Lineage emitted:
        enriched.notional_usd -> risk_report.usd_exposure [transform=exposure_rollup]
        risk_report.usd_exposure -> risk_report.eur_exposure [transform=eur_conversion]
    """

    name = "ExposureRollup"
    version = "1.0.0"
    stage = "enrich"

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        required = ["notional_usd", "counterparty_id", "trade_date"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning("ExposureRollup: missing columns %s — skipping.", missing)
            return df

        # Counterparty-level aggregates via window (keeps row granularity)
        cpty_window = Window.partitionBy("counterparty_id")
        asset_window = Window.partitionBy("asset_class") if "asset_class" in df.columns else cpty_window
        running_window = (
            Window.partitionBy("counterparty_id")
            .orderBy("trade_date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        df = (
            df
            .withColumn("cpty_total_notional_usd",  F.sum("notional_usd").over(cpty_window))
            .withColumn("cpty_trade_count",          F.count("*").over(cpty_window))
            .withColumn("running_notional_usd",      F.sum("notional_usd").over(running_window))
        )

        if "asset_class" in df.columns:
            df = df.withColumn(
                "asset_class_exposure_usd", F.sum("notional_usd").over(asset_window)
            )

        if self.tracker:
            self.tracker.emit(
                source="enriched.notional_usd",
                target="risk_report.usd_exposure",
                transform="exposure_rollup",
                stage=self.stage,
            )
            # If notional_eur exists, extend the lineage chain
            if "notional_eur" in df.columns:
                self.tracker.emit(
                    source="risk_report.usd_exposure",
                    target="risk_report.eur_exposure",
                    transform="eur_conversion",
                    stage=self.stage,
                )

        logger.debug("ExposureRollup | added cpty/asset exposure columns")
        return df

    def validate_schema(self, df: DataFrame) -> List[str]:
        required = ["notional_usd", "counterparty_id"]
        return [f"ExposureRollup requires '{c}'" for c in required if c not in df.columns]


# ---------------------------------------------------------------------------
# PositionNet
# ---------------------------------------------------------------------------

@register_transform("PositionNet")
class PositionNet(BaseTransform):
    """
    Net long and short positions by product within a counterparty.

    For each (counterparty_id, product_id) pair:
        net_notional_usd = SUM(notional_usd WHERE direction='BUY')
                         - SUM(notional_usd WHERE direction='SELL')

    Adds columns:
        buy_notional_usd  : total BUY-side notional
        sell_notional_usd : total SELL-side notional
        net_notional_usd  : net position (positive = long)

    Lineage emitted:
        enriched.notional_usd -> enriched.net_notional_usd [transform=position_net]
    """

    name = "PositionNet"
    version = "1.0.0"
    stage = "enrich"

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        required = ["notional_usd", "direction", "counterparty_id", "product_id"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning("PositionNet: missing columns %s — skipping.", missing)
            return df

        partition_cols = ["counterparty_id", "product_id"]
        window = Window.partitionBy(*partition_cols)

        df = (
            df
            .withColumn(
                "buy_notional_usd",
                F.sum(
                    F.when(F.upper(F.col("direction")) == "BUY", F.col("notional_usd"))
                     .otherwise(F.lit(0.0))
                ).over(window)
            )
            .withColumn(
                "sell_notional_usd",
                F.sum(
                    F.when(F.upper(F.col("direction")) == "SELL", F.col("notional_usd"))
                     .otherwise(F.lit(0.0))
                ).over(window)
            )
            .withColumn(
                "net_notional_usd",
                (F.col("buy_notional_usd") - F.col("sell_notional_usd")).cast(DoubleType()),
            )
        )

        if self.tracker:
            self.tracker.emit(
                source="enriched.notional_usd",
                target="enriched.net_notional_usd",
                transform="position_net",
                stage=self.stage,
                metadata={"partition_cols": partition_cols},
            )

        logger.debug("PositionNet | computed net positions per counterparty/product")
        return df

    def validate_schema(self, df: DataFrame) -> List[str]:
        required = ["notional_usd", "direction"]
        return [f"PositionNet requires '{c}'" for c in required if c not in df.columns]