"""Bootstrap demo DuckDB data for dashboard-only deployments."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DUCKDB_PATH = ROOT_DIR / os.environ.get("DUCKDB_PATH", "data/pipeline.duckdb")


def ensure_demo_database(duckdb_path: Optional[str | Path] = None) -> Path:
    """Create a small dashboard-ready DuckDB database if one is not present."""
    db_path = Path(duckdb_path) if duckdb_path else DEFAULT_DUCKDB_PATH
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path
    if db_path.exists():
        return db_path

    trades_path = _latest_csv("trades_*.csv")
    if trades_path is None:
        return db_path

    db_path.parent.mkdir(parents=True, exist_ok=True)
    _build_demo_database(db_path, trades_path)
    return db_path


def _latest_csv(pattern: str) -> Optional[Path]:
    raw_dir = ROOT_DIR / "data" / "raw"
    matches = sorted(raw_dir.glob(pattern))
    return matches[-1] if matches else None


def _build_demo_database(db_path: Path, trades_path: Path) -> None:
    import duckdb

    con = duckdb.connect(str(db_path))
    schema_path = ROOT_DIR / "models" / "star_schema.sql"
    if schema_path.exists():
        con.execute(schema_path.read_text())

    trades_sql_path = str(trades_path).replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW raw_trades AS
        SELECT * FROM read_csv_auto('{trades_sql_path}', header = true)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW clean_trades AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY trade_date DESC) AS rn
            FROM raw_trades
            WHERE trade_id IS NOT NULL
        )
        WHERE rn = 1
        """
    )

    raw_count = con.execute("SELECT COUNT(*) FROM raw_trades").fetchone()[0]
    clean_count = con.execute("SELECT COUNT(*) FROM clean_trades").fetchone()[0]
    duplicate_count = raw_count - clean_count

    _seed_dimensions(con)
    _seed_facts(con)
    _seed_observability(con, raw_count, clean_count, duplicate_count)
    con.close()


def _seed_dimensions(con) -> None:
    con.execute(
        """
        INSERT OR IGNORE INTO dim_product (
            product_key, product_id, ticker, asset_class, currency, exchange
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_id),
            product_id,
            product_id,
            COALESCE(MAX(asset_class), 'UNKNOWN'),
            COALESCE(MAX(currency), 'USD'),
            COALESCE(MAX(exchange), 'UNKNOWN')
        FROM clean_trades
        WHERE product_id IS NOT NULL
        GROUP BY product_id
        """
    )
    con.execute(
        """
        INSERT OR IGNORE INTO dim_counterparty (
            counterparty_key, counterparty_id, legal_entity_name, lei_code,
            lei_country, credit_rating
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY counterparty_id),
            counterparty_id,
            counterparty_id,
            NULL,
            NULL,
            'A'
        FROM clean_trades
        WHERE counterparty_id IS NOT NULL
        GROUP BY counterparty_id
        """
    )
    con.execute(
        """
        INSERT OR IGNORE INTO dim_date
        SELECT DISTINCT
            CAST(STRFTIME(CAST(date_value AS DATE), '%Y%m%d') AS INTEGER),
            CAST(date_value AS DATE),
            YEAR(CAST(date_value AS DATE)),
            QUARTER(CAST(date_value AS DATE)),
            MONTH(CAST(date_value AS DATE)),
            STRFTIME(CAST(date_value AS DATE), '%B'),
            CAST(STRFTIME(CAST(date_value AS DATE), '%u') AS INTEGER),
            STRFTIME(CAST(date_value AS DATE), '%A'),
            CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)),
            MONTH(CAST(date_value AS DATE)) IN (3, 6, 9, 12)
                AND CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)),
            MONTH(CAST(date_value AS DATE)) = 12
                AND CAST(date_value AS DATE) = LAST_DAY(CAST(date_value AS DATE)),
            CAST(STRFTIME(CAST(date_value AS DATE), '%u') AS INTEGER) BETWEEN 1 AND 5,
            YEAR(CAST(date_value AS DATE)),
            QUARTER(CAST(date_value AS DATE))
        FROM (
            SELECT trade_date AS date_value FROM clean_trades
            UNION
            SELECT settlement_date AS date_value FROM clean_trades
        )
        WHERE date_value IS NOT NULL
        """
    )


def _seed_facts(con) -> None:
    con.execute(
        """
        INSERT INTO fact_trades (
            trade_key, product_key, counterparty_key, trade_date_key,
            settlement_date_key, status_key, trade_id, notional, quantity,
            currency, direction, notional_usd, notional_eur, fx_rate_to_usd,
            batch_id, pipeline_name
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY t.trade_id),
            p.product_key,
            c.counterparty_key,
            CAST(STRFTIME(CAST(t.trade_date AS DATE), '%Y%m%d') AS INTEGER),
            CAST(STRFTIME(CAST(t.settlement_date AS DATE), '%Y%m%d') AS INTEGER),
            COALESCE(s.status_key, 5),
            t.trade_id,
            COALESCE(TRY_CAST(t.notional AS DOUBLE), 0.0),
            COALESCE(TRY_CAST(t.quantity AS DOUBLE), 0.0),
            COALESCE(t.currency, 'USD'),
            COALESCE(t.direction, 'UNKNOWN'),
            COALESCE(TRY_CAST(t.notional AS DOUBLE), 0.0) *
                CASE COALESCE(t.currency, 'USD')
                    WHEN 'EUR' THEN 1.085
                    WHEN 'GBP' THEN 1.272
                    WHEN 'JPY' THEN 0.0067
                    WHEN 'CHF' THEN 1.112
                    WHEN 'CAD' THEN 0.738
                    WHEN 'AUD' THEN 0.653
                    ELSE 1.0
                END,
            COALESCE(TRY_CAST(t.notional AS DOUBLE), 0.0),
            CASE COALESCE(t.currency, 'USD')
                WHEN 'EUR' THEN 1.085
                WHEN 'GBP' THEN 1.272
                WHEN 'JPY' THEN 0.0067
                WHEN 'CHF' THEN 1.112
                WHEN 'CAD' THEN 0.738
                WHEN 'AUD' THEN 0.653
                ELSE 1.0
            END,
            'demo_cloud',
            'streamlit_demo'
        FROM clean_trades t
        LEFT JOIN dim_product p ON p.product_id = t.product_id
        LEFT JOIN dim_counterparty c ON c.counterparty_id = t.counterparty_id
        LEFT JOIN dim_settlement_status s ON s.status_code = COALESCE(t.settlement_status, 'UNKNOWN')
        """
    )


def _seed_observability(con, raw_count: int, clean_count: int, duplicate_count: int) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            batch_id VARCHAR, pipeline_name VARCHAR, started_at TIMESTAMP,
            stage_name VARCHAR, status VARCHAR, records_in INTEGER,
            records_out INTEGER, records_dead INTEGER, duration_sec DOUBLE,
            error VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS job_runs (
            batch_id VARCHAR, job_name VARCHAR, status VARCHAR, command VARCHAR,
            started_at VARCHAR, finished_at VARCHAR, duration_sec DOUBLE,
            attempt INTEGER, exit_code INTEGER, error VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_violations (
            batch_id VARCHAR, rule_name VARCHAR, severity VARCHAR, field_name VARCHAR,
            offending_value VARCHAR, row_key VARCHAR, stage VARCHAR, ts VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS lineage_edges (
            batch_id VARCHAR, source VARCHAR, target VARCHAR, transform VARCHAR,
            stage VARCHAR, ts VARCHAR, metadata VARCHAR
        )
        """
    )

    rows = [
        ("demo_cloud", "streamlit_demo", "2026-05-20 00:00:00", "ingest", "SUCCESS", raw_count, raw_count, 0, 0.2, None),
        ("demo_cloud", "streamlit_demo", "2026-05-20 00:00:01", "clean", "SUCCESS", raw_count, clean_count, duplicate_count, 1.1, None),
        ("demo_cloud", "streamlit_demo", "2026-05-20 00:00:02", "enrich", "SUCCESS", clean_count, clean_count, duplicate_count, 1.3, None),
        ("demo_cloud", "streamlit_demo", "2026-05-20 00:00:03", "load", "SUCCESS", clean_count, clean_count, duplicate_count, 0.8, None),
    ]
    con.executemany("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(
        """
        INSERT INTO job_runs VALUES
        ('demo_cloud', 'daily_trade_pipeline', 'SUCCESS', 'streamlit demo bootstrap',
         '2026-05-20T00:00:00', '2026-05-20T00:00:04', 4.0, 1, 0, NULL)
        """
    )
    con.execute(
        """
        INSERT INTO dq_scores VALUES
        ('demo_cloud', 'post_ingest', 9, 8, 1, 4, 0, 2, 2, 72.5, 0.3, '2026-05-20T00:00:01'),
        ('demo_cloud', 'post_clean', 6, 4, 2, ?, 0, ?, ?, 68.0, 0.4, '2026-05-20T00:00:02'),
        ('demo_cloud', 'pre_load', 6, 6, 0, 0, 0, 0, 0, 100.0, 0.2, '2026-05-20T00:00:03')
        """,
        [duplicate_count, max(duplicate_count // 2, 0), duplicate_count - max(duplicate_count // 2, 0)],
    )
    con.execute(
        """
        INSERT INTO dq_violations
        SELECT 'demo_cloud', 'duplicate_trade_id', 'WARN', 'trade_id',
               trade_id, trade_id, 'post_clean', '2026-05-20T00:00:02'
        FROM raw_trades
        WHERE trade_id IN (
            SELECT trade_id FROM raw_trades GROUP BY trade_id HAVING COUNT(*) > 1
        )
        LIMIT 100
        """
    )
    con.executemany(
        "INSERT INTO lineage_edges VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("demo_cloud", "raw.notional", "clean.notional", "null_fill", "clean", "2026-05-20T00:00:01", "{}"),
            ("demo_cloud", "clean.notional", "enriched.notional_usd", "fx_convert", "enrich", "2026-05-20T00:00:02", "{}"),
            ("demo_cloud", "enriched.notional_usd", "risk_report.usd_exposure", "exposure_rollup", "enrich", "2026-05-20T00:00:02", "{}"),
            ("demo_cloud", "risk_report.usd_exposure", "fact_trades.notional_usd", "write_star_schema", "load", "2026-05-20T00:00:03", "{}"),
            ("demo_cloud", "raw.counterparty_id", "enriched.lei_code", "lei_lookup", "enrich", "2026-05-20T00:00:02", "{}"),
        ],
    )
