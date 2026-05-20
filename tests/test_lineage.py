from __future__ import annotations

from etl.lineage_tracker import LineageTracker
from graph.queries import trace_ancestry


def test_lineage_persists_and_queries_duckdb(tmp_path):
    db_path = tmp_path / "pipeline.duckdb"
    tracker = LineageTracker()
    tracker.emit("raw.notional", "enriched.notional_usd", "fx_convert", "enrich")
    tracker.emit("enriched.notional_usd", "risk_report.usd_exposure", "exposure_rollup", "enrich")

    tracker.save_to_duckdb(str(db_path), "b_test")

    assert trace_ancestry("risk_report.usd_exposure", str(db_path), "b_test") == [
        "raw.notional",
        "enriched.notional_usd",
    ]
