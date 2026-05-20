from __future__ import annotations

from jobs.job_executor import JobExecutor


def test_job_executor_orders_dependencies(tmp_path):
    definitions = tmp_path / "jobs.json"
    definitions.write_text(
        """
{
  "jobs": {
    "a": {"type": "command", "command": "echo a", "depends_on": []},
    "b": {"type": "command", "command": "echo b", "depends_on": ["a"]},
    "all": {"type": "group", "members": ["b"], "depends_on": ["b"]}
  }
}
""",
        encoding="utf-8",
    )
    config = tmp_path / "pipeline_config.yaml"
    config.write_text("duckdb_path: data/pipeline.duckdb\n", encoding="utf-8")

    executor = JobExecutor(definitions_path=definitions, pipeline_config_path=config)

    assert executor._execution_order("all") == ["a", "b", "all"]
