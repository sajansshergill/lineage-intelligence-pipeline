"""Autosys-style job executor for the local lineage pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

logger = logging.getLogger(__name__)

JOB_DEFINITIONS_PATH = Path("jobs/job_definitions.json")
PIPELINE_CONFIG_PATH = Path("config/pipeline_config.yaml")

# ------------------------------------------------------------
# Job status constants
# ------------------------------------------------------------

class JobStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    SKIPPED = "SKIPPED"
    TIMEOUT = "TIMEOUT"
    FAILED = "FAILED"
    
# ------------------------------------------------------------
# Job run record
# ------------------------------------------------------------

@dataclass
class JobRun:
    job_name: str
    batch_id: str
    status: str = JobStatus.PENDING
    command: Optional[str] = None
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: Optional[str] = None
    duration_sec: float = 0.0
    attempt: int = 0
    exit_code: Optional[int] = None
    error: Optional[str] = None


class JobExecutor:
    def __init__(
        self,
        definitions_path: Path = JOB_DEFINITIONS_PATH,
        pipeline_config_path: Path = PIPELINE_CONFIG_PATH,
    ):
        self.definitions_path = definitions_path
        self.pipeline_config_path = pipeline_config_path
        self.jobs = self._load_jobs()
        self.duckdb_path = self._load_duckdb_path()
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    def list_jobs(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": name,
                "type": spec.get("type", "command"),
                "depends_on": spec.get("depends_on", []),
                "description": spec.get("description", ""),
            }
            for name, spec in sorted(self.jobs.items())
        ]

    def run(self, job_name: str, batch_id: Optional[str] = None, dry_run: bool = False) -> Dict[str, JobRun]:
        if job_name not in self.jobs:
            raise KeyError(f"Unknown job '{job_name}'. Available: {', '.join(sorted(self.jobs))}")

        batch_id = batch_id or self._new_batch_id()
        context = self._runtime_context(batch_id)
        order = self._execution_order(job_name)
        results: Dict[str, JobRun] = {}

        logger.info("Job stream starting | root=%s batch=%s order=%s", job_name, batch_id, order)
        for name in order:
            spec = self.jobs[name]
            upstream = spec.get("depends_on", [])
            blocked = [dep for dep in upstream if results.get(dep) and results[dep].status != JobStatus.SUCCESS]
            if blocked:
                run = JobRun(
                    job_name=name,
                    batch_id=batch_id,
                    status=JobStatus.SKIPPED,
                    error=f"Skipped because upstream jobs failed: {blocked}",
                )
                self._finish_run(run)
                results[name] = run
                self._write_history(run)
                continue

            if spec.get("type") == "group":
                run = JobRun(job_name=name, batch_id=batch_id, status=JobStatus.SUCCESS)
                self._finish_run(run)
                results[name] = run
                self._write_history(run)
                continue

            run = self._run_command_job(name=name, spec=spec, context=context, dry_run=dry_run)
            results[name] = run
            self._write_history(run)

        return results

    def _load_jobs(self) -> Dict[str, Dict[str, Any]]:
        if not self.definitions_path.exists():
            raise FileNotFoundError(f"Job definitions not found: {self.definitions_path}")
        with open(self.definitions_path) as f:
            raw = json.load(f)
        jobs = raw.get("jobs", raw if isinstance(raw, dict) else {})
        if not isinstance(jobs, dict) or not jobs:
            raise ValueError(f"No jobs defined in {self.definitions_path}")
        return jobs

    def _load_duckdb_path(self) -> Path:
        if not self.pipeline_config_path.exists():
            return Path("data/pipeline.duckdb")
        with open(self.pipeline_config_path) as f:
            config = yaml.safe_load(f) or {}
        return Path(config.get("duckdb_path", "data/pipeline.duckdb"))

    def _execution_order(self, root_job: str) -> List[str]:
        expanded = self._expand_group(root_job)
        needed: Set[str] = set()
        visiting: Set[str] = set()
        visited: Set[str] = set()
        order: List[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                raise ValueError(f"Cycle detected in job dependencies at '{name}'")
            if name not in self.jobs:
                raise KeyError(f"Job '{name}' referenced but not defined")
            visiting.add(name)
            for dep in self.jobs[name].get("depends_on", []):
                visit(dep)
            visiting.remove(name)
            visited.add(name)
            needed.add(name)
            order.append(name)

        for name in expanded:
            visit(name)
        return order

    def _expand_group(self, job_name: str) -> List[str]:
        spec = self.jobs[job_name]
        if spec.get("type") != "group":
            return [job_name]
        members = spec.get("members", [])
        if not members:
            raise ValueError(f"Group job '{job_name}' has no members")
        return members + [job_name]

    def _run_command_job(
        self,
        name: str,
        spec: Dict[str, Any],
        context: Dict[str, str],
        dry_run: bool,
    ) -> JobRun:
        command = self._render_command(spec["command"], context)
        attempts = int(spec.get("retry_attempts", 0)) + 1
        timeout = int(spec.get("max_runtime_sec", spec.get("max_runtime", 3600)))
        run = JobRun(job_name=name, batch_id=context["BATCH_ID"], command=command)

        for attempt in range(1, attempts + 1):
            run.attempt = attempt
            t0 = time.perf_counter()
            run.status = JobStatus.RUNNING
            logger.info("Running job '%s' attempt %d/%d: %s", name, attempt, attempts, command)

            if dry_run:
                run.status = JobStatus.SUCCESS
                run.error = "dry-run"
                self._finish_run(run, started=t0)
                return run

            try:
                env = os.environ.copy()
                env.setdefault("PYSPARK_PYTHON", sys.executable)
                env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
                proc = subprocess.run(
                    command,
                    shell=True,
                    cwd=Path.cwd(),
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                )
                run.exit_code = proc.returncode
                if proc.returncode == 0:
                    run.status = JobStatus.SUCCESS
                    self._finish_run(run, started=t0)
                    return run
                run.status = JobStatus.FAILED
                run.error = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}")[-4000:]
            except subprocess.TimeoutExpired as exc:
                run.status = JobStatus.TIMEOUT
                run.error = f"Timed out after {timeout}s: {exc}"
            except Exception as exc:
                run.status = JobStatus.FAILED
                run.error = str(exc)

            self._finish_run(run, started=t0)
            if attempt < attempts:
                time.sleep(min(30, 2 ** (attempt - 1)))

        logger.error("Job '%s' failed: %s", name, run.error)
        return run

    def _write_history(self, run: JobRun) -> None:
        try:
            import duckdb
            con = duckdb.connect(str(self.duckdb_path))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS job_runs (
                    batch_id VARCHAR,
                    job_name VARCHAR,
                    status VARCHAR,
                    command VARCHAR,
                    started_at VARCHAR,
                    finished_at VARCHAR,
                    duration_sec DOUBLE,
                    attempt INTEGER,
                    exit_code INTEGER,
                    error VARCHAR
                )
                """
            )
            con.execute(
                "INSERT INTO job_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run.batch_id,
                    run.job_name,
                    run.status,
                    run.command,
                    run.started_at,
                    run.finished_at,
                    run.duration_sec,
                    run.attempt,
                    run.exit_code,
                    run.error,
                ),
            )
            con.close()
        except Exception as exc:
            logger.warning("Could not write job history: %s", exc)

    def _runtime_context(self, batch_id: str) -> Dict[str, str]:
        run_date = datetime.now(UTC).strftime("%Y%m%d")
        return {
            "BATCH_ID": batch_id,
            "RUN_DATE": run_date,
            "TRADE_FILE": f"data/raw/trades_{run_date}.csv",
        }

    @staticmethod
    def _render_command(command: str, context: Dict[str, str]) -> str:
        return command.format(**context)

    @staticmethod
    def _new_batch_id() -> str:
        return f"b_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

    @staticmethod
    def _finish_run(run: JobRun, started: Optional[float] = None) -> None:
        run.finished_at = datetime.now(UTC).isoformat()
        if started is not None:
            run.duration_sec = time.perf_counter() - started


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run lineage pipeline jobs")
    parser.add_argument("--job", help="Root job or group job to run")
    parser.add_argument("--batch", help="Batch id override")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and log jobs without executing commands")
    parser.add_argument("--list", action="store_true", help="List configured jobs")
    parser.add_argument("--definitions", default=str(JOB_DEFINITIONS_PATH))
    args = parser.parse_args(argv)

    executor = JobExecutor(definitions_path=Path(args.definitions))
    if args.list:
        for job in executor.list_jobs():
            deps = ",".join(job["depends_on"]) or "-"
            print(f"{job['name']:24s} type={job['type']:8s} depends_on={deps} {job['description']}")
        return 0
    if not args.job:
        parser.error("--job is required unless --list is used")

    results = executor.run(job_name=args.job, batch_id=args.batch, dry_run=args.dry_run)
    print("\n=== Job Summary ===")
    for name, run in results.items():
        print(f"  {name:24s} | {run.status:8s} | attempt={run.attempt} | {run.duration_sec:.2f}s")
    return 0 if all(run.status in {JobStatus.SUCCESS, JobStatus.SKIPPED} for run in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())