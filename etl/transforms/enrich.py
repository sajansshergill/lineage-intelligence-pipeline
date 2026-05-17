"""
etl/transforms/enrich.py
------------------------
Enrichment transforms: FxConvert, DateExpand, LeiLookup.

These transforms add derived/lookup columns to the clean trade DataFrame.
Each emits precise lineage edges so the dashboard can answer:
  "Which source field is responsible for the EUR exposure figure?"
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, StringType,
)

from etl.framework import BaseTransform, BatchMeta
from etl.transforms.registry import register_transform

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Static FX rates (used when a live API is unavailable)
# ---------------------------------------------------------------------------

# Mid-market rates relative to USD — replace with live lookup in production
_STATIC_FX_RATES_TO_USD: Dict[str, float] = {
    "USD": 1.0000,
    "EUR": 1.0850,
    "GBP": 1.2720,
    "JPY": 0.0067,
    "CHF": 1.1120,
    "CAD": 0.7380,
    "AUD": 0.6530,
    "HKD": 0.1280,
    "SGD": 0.7490,
    "CNY": 0.1380,
}


# ---------------------------------------------------------------------------
# FxConvert
# ---------------------------------------------------------------------------

@register_transform("FxConvert")
class FxConvert(BaseTransform):
    """
    Convert trade notional from original currency to USD and EUR.

    Adds columns:
        notional_usd   : notional * fx_rate_to_usd
        notional_eur   : notional * fx_rate_to_usd / eur_rate
        fx_rate_to_usd : rate applied for audit

    Config:
        fx_convert:
          source_col: notional
          currency_col: currency
          rates_override:         # optional per-currency overrides
            JPY: 0.0068

    Lineage emitted (core path):
        clean.notional  -> enriched.notional_usd  [transform=fx_convert]
        enriched.notional_usd -> enriched.notional_eur [transform=fx_to_eur]
    """

    name = "FxConvert"
    version = "1.0.0"
    stage = "enrich"

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        cfg = self.config.get("fx_convert", {})
        source_col: str = cfg.get("source_col", "notional")
        currency_col: str = cfg.get("currency_col", "currency")

        if source_col not in df.columns:
            logger.warning("FxConvert: source column '%s' not found — skipping.", source_col)
            return df

        # Merge static rates with any config overrides
        rates = {**_STATIC_FX_RATES_TO_USD, **cfg.get("rates_override", {})}
        eur_rate = rates.get("EUR", 1.0850)

        # Build a PySpark CASE WHEN expression for the rate lookup
        rate_expr = F.lit(1.0)   # default: assume USD if currency unknown
        for currency, rate in rates.items():
            rate_expr = F.when(
                F.upper(F.col(currency_col)) == currency, F.lit(rate)
            ).otherwise(rate_expr)

        df = (
            df
            .withColumn("fx_rate_to_usd", rate_expr)
            .withColumn(
                "notional_usd",
                (F.col(source_col) * F.col("fx_rate_to_usd")).cast(DoubleType()),
            )
            .withColumn(
                "notional_eur",
                (F.col("notional_usd") / F.lit(eur_rate)).cast(DoubleType()),
            )
        )

        if self.tracker:
            self.tracker.emit(
                source=f"clean.{source_col}",
                target="enriched.notional_usd",
                transform="fx_convert",
                stage=self.stage,
                metadata={
                    "currency_col": currency_col,
                    "rate_source": "static_fallback",
                },
            )
            self.tracker.emit(
                source="enriched.notional_usd",
                target="enriched.notional_eur",
                transform="fx_to_eur",
                stage=self.stage,
                metadata={"eur_rate": eur_rate},
            )

        logger.debug("FxConvert | added notional_usd, notional_eur, fx_rate_to_usd")
        return df

    def validate_schema(self, df: DataFrame) -> List[str]:
        required = [
            self.config.get("fx_convert", {}).get("source_col", "notional"),
            self.config.get("fx_convert", {}).get("currency_col", "currency"),
        ]
        return [f"FxConvert requires '{c}'" for c in required if c not in df.columns]


# ---------------------------------------------------------------------------
# DateExpand
# ---------------------------------------------------------------------------

@register_transform("DateExpand")
class DateExpand(BaseTransform):
    """
    Expand a trade_date timestamp into calendar dimension attributes.
    These columns map directly to DimDate in the star schema.

    Adds columns:
        trade_year, trade_quarter, trade_month, trade_day_of_week,
        is_month_end, days_to_settlement, is_trading_day (placeholder)

    Lineage emitted:
        clean.trade_date -> enriched.trade_year   [transform=date_expand]
        clean.trade_date -> enriched.trade_quarter [transform=date_expand]
        clean.trade_date -> enriched.days_to_settlement [transform=date_expand]
    """

    name = "DateExpand"
    version = "1.0.0"
    stage = "enrich"

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        date_col = self.config.get("date_expand", {}).get("date_col", "trade_date")
        settle_col = self.config.get("date_expand", {}).get("settle_col", "settlement_date")

        if date_col not in df.columns:
            logger.warning("DateExpand: '%s' not found — skipping.", date_col)
            return df

        df = (
            df
            .withColumn("trade_year",        F.year(date_col))
            .withColumn("trade_quarter",      F.quarter(date_col))
            .withColumn("trade_month",        F.month(date_col))
            .withColumn("trade_day_of_week",  F.dayofweek(date_col))
            .withColumn(
                "is_month_end",
                (F.dayofmonth(date_col) == F.dayofmonth(F.last_day(date_col))).cast(BooleanType()),
            )
            .withColumn(
                "is_trading_day",
                # Weekday check — Mon=2, Fri=6 in Spark's dayofweek (Sun=1)
                F.col("trade_day_of_week").between(2, 6).cast(BooleanType()),
            )
        )

        # Days to settlement (only if settlement_date present)
        if settle_col in df.columns:
            df = df.withColumn(
                "days_to_settlement",
                F.datediff(F.col(settle_col), F.col(date_col)),
            )

            if self.tracker:
                self.tracker.emit(
                    source=f"clean.{date_col}",
                    target="enriched.days_to_settlement",
                    transform="date_expand",
                    stage=self.stage,
                )

        derived_cols = [
            "trade_year", "trade_quarter", "trade_month",
            "trade_day_of_week", "is_month_end", "is_trading_day",
        ]

        if self.tracker:
            for derived in derived_cols:
                self.tracker.emit(
                    source=f"clean.{date_col}",
                    target=f"enriched.{derived}",
                    transform="date_expand",
                    stage=self.stage,
                )

        logger.debug("DateExpand | added %s", derived_cols)
        return df


# ---------------------------------------------------------------------------
# LeiLookup
# ---------------------------------------------------------------------------

@register_transform("LeiLookup")
class LeiLookup(BaseTransform):
    """
    Enrich counterparty rows with Legal Entity Identifier (LEI) metadata.

    In production this hits the GLEIF API or an internal reference database.
    Here we use a static lookup table for demonstration; wired to an external
    source via config.

    Adds columns:
        lei_code, legal_entity_name, lei_country, credit_rating

    Config:
        lei_lookup:
          counterparty_col: counterparty_id
          lookup_path: data/schema/lei_reference.csv   # optional override
          broadcast: true                               # broadcast hint

    Lineage emitted:
        clean.counterparty_id -> enriched.lei_code [transform=lei_lookup]
        clean.counterparty_id -> enriched.legal_entity_name [transform=lei_lookup]
    """

    name = "LeiLookup"
    version = "1.0.0"
    stage = "enrich"

    _STATIC_LEI: Dict[str, Dict[str, str]] = {
        "CP001": {"lei_code": "5493000IBP32UQZ0KL24", "legal_entity_name": "Alpha Capital LLC",   "lei_country": "US", "credit_rating": "AA"},
        "CP002": {"lei_code": "213800WSGIIZCXF1P572", "legal_entity_name": "Beta Finance GmbH",   "lei_country": "DE", "credit_rating": "A+"},
        "CP003": {"lei_code": "969500T3MBS4SQAMHJ45", "legal_entity_name": "Gamma Trading Ltd",   "lei_country": "GB", "credit_rating": "A"},
        "CP004": {"lei_code": "254900OPPU84GM83WH09", "legal_entity_name": "Delta Securities SA",  "lei_country": "FR", "credit_rating": "BBB+"},
        "CP005": {"lei_code": "3358003ERQZ8EKGPNB86", "legal_entity_name": "Epsilon Funds Ltd",   "lei_country": "IE", "credit_rating": "AA-"},
    }

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        cfg = self.config.get("lei_lookup", {})
        cpty_col: str = cfg.get("counterparty_col", "counterparty_id")
        use_broadcast: bool = cfg.get("broadcast", True)

        if cpty_col not in df.columns:
            logger.warning("LeiLookup: '%s' not found — skipping.", cpty_col)
            return df

        # Build a small reference DataFrame from the static dict
        spark = df.sparkSession
        rows = [
            (cp_id, v["lei_code"], v["legal_entity_name"], v["lei_country"], v["credit_rating"])
            for cp_id, v in self._STATIC_LEI.items()
        ]
        ref_df = spark.createDataFrame(
            rows,
            ["counterparty_id", "lei_code", "legal_entity_name", "lei_country", "credit_rating"],
        )

        if use_broadcast:
            ref_df = F.broadcast(ref_df)

        enriched = df.join(ref_df, on=cpty_col, how="left")

        if self.tracker:
            for target_col in ["lei_code", "legal_entity_name", "lei_country", "credit_rating"]:
                self.tracker.emit(
                    source=f"clean.{cpty_col}",
                    target=f"enriched.{target_col}",
                    transform="lei_lookup",
                    stage=self.stage,
                    metadata={"lookup_source": "static_lei_table"},
                )

        logger.debug("LeiLookup | joined %d counterparty reference rows", len(rows))
        return enriched