from __future__ import annotations


def test_star_schema_creates_core_tables(tmp_path):
    import duckdb

    db_path = tmp_path / "schema.duckdb"
    con = duckdb.connect(str(db_path))
    with open("models/star_schema.sql") as f:
        con.execute(f.read())

    tables = {
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }
    con.close()

    assert {"dim_date", "dim_product", "dim_counterparty", "dim_settlement_status", "fact_trades"} <= tables
