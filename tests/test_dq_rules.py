from __future__ import annotations

from dq.rules_engine import DQRulesEngine


def test_dq_checkpoint_records_score(tmp_path, spark):
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        """
rules:
  - name: trade_id_not_null
    field: trade_id
    type: null_check
    severity: CRITICAL
    stage: post_ingest
    row_key_col: trade_id
""",
        encoding="utf-8",
    )
    df = spark.createDataFrame([("T1",), (None,)], ["trade_id"])
    engine = DQRulesEngine(config_path=str(rules_path), duckdb_path=str(tmp_path / "dq.duckdb"))

    result = engine.run_checkpoint(df, checkpoint="post_ingest", batch_id="b_test")

    assert result.rules_run == 1
    assert result.rules_failed == 1
    assert result.total_violations == 1
