"""Load transforms for DuckDB-backed analytical tables."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame

from etl.framework import BaseTransform, BatchMeta
from etl.transforms.registry import register_transform

logger = logging.getLogger(__name__)


@register_transform("WriteStarSchema")
class WriteStarSchema(BaseTransform):
    """Append enriched trade rows to the local DuckDB star schema."""

    name = "WriteStarSchema"
    version = "1.0.0"
    stage = "load"

    REQUIRED_COLUMNS = [
        "trade_id",
        "product_id",
        "counterparty_id",
        "trade_date",
        "settlement_date",
        "settlement_status",
        "notional",
        "quantity",
        "currency",
        "direction",
    ]

    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        duckdb_path = self.config.get("duckdb_path", "data/pipeline.duckdb")
        schema_path = self.config.get("star_schema_path", "models/star_schema.sql")
        Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)

        pandas_df = df.toPandas()
        if pandas_df.empty:
            logger.info("WriteStarSchema | no rows to load for batch=%s", meta.batch_id)
            return df

        try:
            import duckdb

            con = duckdb.connect(duckdb_path)
            with open(schema_path) as f:
                con.execute(f.read())
            con.register("stage_trades_df", pandas_df)
            con.execute(
                """
                INSERT OR IGNORE INTO dim_product (
                    product_key, product_id, ticker, asset_class, currency, exchange
                )
                SELECT
                    COALESCE((SELECT MAX(product_key) FROM dim_product), 0)
                        + ROW_NUMBER() OVER (ORDER BY staged.product_id),
                    staged.product_id,
                    staged.product_id,
                    staged.asset_class,
                    staged.currency,
                    staged.exchange
                FROM (
                    SELECT
                        product_id,
                        COALESCE(MAX(asset_class), 'UNKNOWN') AS asset_class,
                        COALESCE(MAX(currency), 'USD') AS currency,
                        COALESCE(MAX(exchange), 'UNKNOWN') AS exchange
                    FROM stage_trades_df
                    WHERE product_id IS NOT NULL
                    GROUP BY product_id
                ) staged
                LEFT JOIN dim_product existing
                  ON existing.product_id = staged.product_id
                WHERE existing.product_id IS NULL
                """
            )
            con.execute(
                """
                INSERT OR IGNORE INTO dim_counterparty (
                    counterparty_key, counterparty_id, legal_entity_name,
                    lei_code, lei_country, credit_rating
                )
                SELECT
                    COALESCE((SELECT MAX(counterparty_key) FROM dim_counterparty), 0)
                        + ROW_NUMBER() OVER (ORDER BY staged.counterparty_id),
                    staged.counterparty_id,
                    staged.legal_entity_name,
                    staged.lei_code,
                    staged.lei_country,
                    staged.credit_rating
                FROM (
                    SELECT
                        counterparty_id,
                        COALESCE(MAX(legal_entity_name), counterparty_id) AS legal_entity_name,
                        MAX(lei_code) AS lei_code,
                        MAX(lei_country) AS lei_country,
                        MAX(credit_rating) AS credit_rating
                    FROM stage_trades_df
                    WHERE counterparty_id IS NOT NULL
                    GROUP BY counterparty_id
                ) staged
                LEFT JOIN dim_counterparty existing
                  ON existing.counterparty_id = staged.counterparty_id
                WHERE existing.counterparty_id IS NULL
                """
            )
            con.execute(
                """
                INSERT OR IGNORE INTO dim_date
                SELECT DISTINCT
                    CAST(STRFTIME(CAST(date_value AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
                    CAST(date_value AS DATE) AS calendar_date,
                    YEAR(CAST(date_value AS DATE)) AS year,
                    QUARTER(CAST(date_value AS DATE)) AS quarter,
                    MONTH(CAST(date_value AS DATE)) AS month,
                    STRFTIME(CAST(date_value AS DATE), '%B') AS month_name,
                    CAST(STRFTIME(CAST(date_value AS DATE), '%u') AS INTEGER) AS day_of_week,
                    STRFTIME(CAST(date_value AS DATE), '%A') AS day_name,
                    CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)) AS is_month_end,
                    MONTH(CAST(date_value AS DATE)) IN (3, 6, 9, 12)
                        AND CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)) AS is_quarter_end,
                    MONTH(CAST(date_value AS DATE)) = 12
                        AND CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)) AS is_year_end,
                    CAST(STRFTIME(CAST(date_value AS DATE), '%u') AS INTEGER) BETWEEN 1 AND 5 AS is_trading_day,
                    YEAR(CAST(date_value AS DATE)) AS fiscal_year,
                    QUARTER(CAST(date_value AS DATE)) AS fiscal_quarter
                FROM (
                    SELECT trade_date AS date_value FROM stage_trades_df
                    UNION
                    SELECT settlement_date AS date_value FROM stage_trades_df
                )
                WHERE date_value IS NOT NULL
                """
            )
            con.execute(
                """
                DELETE FROM fact_trades
                WHERE trade_id IN (
                    SELECT DISTINCT trade_id
                    FROM stage_trades_df
                    WHERE trade_id IS NOT NULL
                )
                """
            )
            con.execute(
                """
                INSERT INTO fact_trades (
                    trade_key, product_key, counterparty_key, trade_date_key,
                    settlement_date_key, status_key, trade_id, notional, quantity,
                    currency, direction, notional_usd, notional_eur, fx_rate_to_usd,
                    buy_notional_usd, sell_notional_usd, net_notional_usd,
                    cpty_total_notional_usd, cpty_trade_count, running_notional_usd,
                    asset_class_exposure_usd, trade_year, trade_quarter, trade_month,
                    trade_day_of_week, is_month_end, is_trading_day, days_to_settlement,
                    batch_id, pipeline_name
                )
                SELECT
                    COALESCE((SELECT MAX(trade_key) FROM fact_trades), 0)
                        + ROW_NUMBER() OVER (ORDER BY s.trade_id),
                    p.product_key,
                    c.counterparty_key,
                    CAST(STRFTIME(CAST(s.trade_date AS DATE), '%Y%m%d') AS INTEGER),
                    CAST(STRFTIME(CAST(s.settlement_date AS DATE), '%Y%m%d') AS INTEGER),
                    COALESCE(dss.status_key, 5),
                    s.trade_id,
                    s.notional,
                    s.quantity,
                    COALESCE(s.currency, 'USD'),
                    COALESCE(s.direction, 'UNKNOWN'),
                    s.notional_usd,
                    s.notional_eur,
                    s.fx_rate_to_usd,
                    s.buy_notional_usd,
                    s.sell_notional_usd,
                    s.net_notional_usd,
                    s.cpty_total_notional_usd,
                    s.cpty_trade_count,
                    s.running_notional_usd,
                    s.asset_class_exposure_usd,
                    s.trade_year,
                    s.trade_quarter,
                    s.trade_month,
                    s.trade_day_of_week,
                    s.is_month_end,
                    s.is_trading_day,
                    s.days_to_settlement,
                    ?,
                    ?
                FROM stage_trades_df s
                LEFT JOIN dim_product p
                  ON p.product_id = s.product_id
                LEFT JOIN dim_counterparty c
                  ON c.counterparty_id = s.counterparty_id
                LEFT JOIN dim_settlement_status dss
                  ON dss.status_code = COALESCE(s.settlement_status, 'UNKNOWN')
                """,
                [meta.batch_id, meta.pipeline_name],
            )
            con.unregister("stage_trades_df")
            con.close()
        except Exception as exc:
            logger.error("WriteStarSchema failed: %s", exc, exc_info=True)
            raise

        if self.tracker:
            for col in ["notional_usd", "notional_eur", "net_notional_usd"]:
                if col in df.columns:
                    self.tracker.emit(
                        source=f"enriched.{col}",
                        target=f"fact_trades.{col}",
                        transform="write_star_schema",
                        stage=self.stage,
                        metadata={"duckdb_path": duckdb_path},
                    )

        logger.info("WriteStarSchema | loaded %d rows | batch=%s", len(pandas_df), meta.batch_id)
        return df

    def validate_schema(self, df: DataFrame) -> List[str]:
        return [f"WriteStarSchema requires '{col}'" for col in self.REQUIRED_COLUMNS if col not in df.columns]
