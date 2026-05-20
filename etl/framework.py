"""
etl/framework.py
----------------
Core ETL framework: BaseTransform, Pipeline, StageRunner.

Every stage in the pipeline is a named, versioned transform that:
  - Accepts a PySpark DataFrame
  - Emits lineage edges via LineageTracker
  - Returns a transformed DataFrame
  - Routes failed records to the dead-letter queue

Usage:
    pipeline = Pipeline(config_path="config/pipeline_config.yaml")
    pipeline.run(stage="daily_trade_pipeline", input_path="data/raw/trades_20250515.csv")
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow `python etl/framework.py` from the repo root during local development.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from etl.error_handler import DeadLetterQueue, RetryHandler
from etl.lineage_tracker import LineageTracker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------

@dataclass
class BatchMeta:
    """Metadata for a single pipeline run, passed through every stage."""
    batch_id: str
    pipeline_name: str
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    stage_results: Dict[str, "StageResult"] = field(default_factory=dict)
    dq_results: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(cls, pipeline_name: str) -> "BatchMeta":
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        return cls(batch_id=f"b_{ts}", pipeline_name=pipeline_name)


@dataclass
class StageResult:
    stage_name: str
    status: str          # SUCCESS | FAILED | SKIPPED
    records_in: int = 0
    records_out: int = 0
    records_dead: int = 0
    duration_sec: float = 0.0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Abstract base transform
# ---------------------------------------------------------------------------

class BaseTransform(ABC):
    """
    All transforms extend this class.

    Subclasses implement `_transform(df, meta, tracker)` and optionally
    override `validate_schema(df)` for pre-condition checks.
    """

    # Set by subclass or the registry decorator
    name: str = "unnamed_transform"
    version: str = "1.0.0"
    stage: str = "unknown"          # ingest | clean | enrich | load

    def __init__(
        self,
        config: Dict[str, Any] = None,
        dlq: Optional[DeadLetterQueue] = None,
        tracker: Optional[LineageTracker] = None,
    ):
        self.config = config or {}
        self.dlq = dlq
        self.tracker = tracker or LineageTracker()

    # ------------------------------------------------------------------
    # Public entry point — called by StageRunner
    # ------------------------------------------------------------------

    def execute(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        """
        Wraps _transform with timing, error handling, and DLQ routing.
        Records that raise a per-row exception are moved to the dead-letter
        partition; stage-level exceptions propagate after logging.
        """
        logger.info(
            "[%s] Starting transform '%s' v%s | batch=%s | records_in=%d",
            self.stage, self.name, self.version, meta.batch_id, df.count(),
        )
        t0 = time.perf_counter()
        try:
            result_df = self._transform(df, meta)
            elapsed = time.perf_counter() - t0
            logger.info(
                "[%s] Completed '%s' | records_out=%d | %.2fs",
                self.stage, self.name, result_df.count(), elapsed,
            )
            return result_df
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            logger.error(
                "[%s] Transform '%s' FAILED after %.2fs: %s",
                self.stage, self.name, elapsed, exc, exc_info=True,
            )
            # Route entire batch to DLQ on stage failure — downstream
            # stages will not see this data.
            if self.dlq:
                self.dlq.write(df, reason=str(exc), batch_id=meta.batch_id, stage=self.name)
            raise

    @abstractmethod
    def _transform(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        """Implement the actual transformation logic here."""

    def validate_schema(self, df: DataFrame) -> List[str]:
        """
        Optional pre-condition check. Return a list of error strings;
        empty list means the schema is acceptable.
        """
        return []


# ---------------------------------------------------------------------------
# Stage runner — executes a sequence of transforms for one pipeline stage
# ---------------------------------------------------------------------------

class StageRunner:
    """
    Runs an ordered list of BaseTransform instances for a named stage.
    Captures per-stage metrics and writes them to BatchMeta.
    """

    def __init__(
        self,
        stage_name: str,
        transforms: List[BaseTransform],
        dlq: DeadLetterQueue,
        retry_handler: RetryHandler,
    ):
        self.stage_name = stage_name
        self.transforms = transforms
        self.dlq = dlq
        self.retry_handler = retry_handler

    def run(self, df: DataFrame, meta: BatchMeta) -> DataFrame:
        records_in = df.count()
        t0 = time.perf_counter()

        try:
            for transform in self.transforms:
                schema_errors = transform.validate_schema(df)
                if schema_errors:
                    logger.warning(
                        "[%s] Schema validation warnings for '%s': %s",
                        self.stage_name, transform.name, schema_errors,
                    )

                df = self.retry_handler.execute_with_retry(
                    fn=transform.execute,
                    df=df,
                    meta=meta,
                )

            result = StageResult(
                stage_name=self.stage_name,
                status="SUCCESS",
                records_in=records_in,
                records_out=df.count(),
                records_dead=self.dlq.count_for_batch(meta.batch_id),
                duration_sec=time.perf_counter() - t0,
            )
        except Exception as exc:
            result = StageResult(
                stage_name=self.stage_name,
                status="FAILED",
                records_in=records_in,
                records_out=0,
                records_dead=self.dlq.count_for_batch(meta.batch_id),
                duration_sec=time.perf_counter() - t0,
                error=str(exc),
            )
            meta.stage_results[self.stage_name] = result
            raise

        meta.stage_results[self.stage_name] = result
        return df


# ---------------------------------------------------------------------------
# Pipeline — top-level orchestrator
# ---------------------------------------------------------------------------

class Pipeline:
    """
    Reads pipeline_config.yaml and executes stages in declared order.

    Config shape:
        pipeline:
          name: daily_trade_pipeline
          stages:
            - name: ingest
              transforms: [ReadParquet]
              checkpoint: post_ingest
            - name: clean
              transforms: [NullFill, TypeCast, Dedup]
              checkpoint: post_clean
            - name: enrich
              transforms: [FxConvert, DateExpand]
            - name: load
              transforms: [WriteStarSchema]
    """

    def __init__(self, config_path: str, spark: Optional[SparkSession] = None):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.spark = spark or self._build_spark_session()
        self.duckdb_path = self.config.get("duckdb_path", "data/pipeline.duckdb")
        Path(self.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
        self.dlq = DeadLetterQueue(
            spark=self.spark,
            base_path=self.config.get("dlq_path", "data/dead_letter"),
        )
        self.retry_handler = RetryHandler(
            max_attempts=self.config.get("retry", {}).get("max_attempts", 3),
            backoff_base=self.config.get("retry", {}).get("backoff_base_sec", 2.0),
        )
        self.tracker = LineageTracker()
        self.dq_engine = self._build_dq_engine()

        from etl.transforms.registry import _load_all_transforms
        _load_all_transforms()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        input_path: str,
        pipeline_name: Optional[str] = None,
        stage_filter: Optional[str] = None,
    ) -> BatchMeta:
        """
        Execute the full pipeline (or a single named stage).

        Args:
            input_path:    Path to the raw input file (Parquet or CSV).
            pipeline_name: Override the pipeline name from config.
            stage_filter:  If set, run only this stage (for debugging).

        Returns:
            BatchMeta populated with per-stage results.
        """
        name = pipeline_name or self.config["pipeline"]["name"]
        meta = BatchMeta.new(pipeline_name=name)
        self.tracker = LineageTracker()
        logger.info("=== Pipeline '%s' starting | batch=%s ===", name, meta.batch_id)

        df = self._read_input(input_path)
        pipeline_failed = False

        for stage_cfg in self.config["pipeline"]["stages"]:
            sname = stage_cfg["name"]
            if stage_filter and sname != stage_filter:
                logger.info("Skipping stage '%s' (filter=%s)", sname, stage_filter)
                continue

            transforms = self._instantiate_transforms(
                stage_cfg.get("transforms", []),
                tracker=self.tracker,
            )
            runner = StageRunner(
                stage_name=sname,
                transforms=transforms,
                dlq=self.dlq,
                retry_handler=self.retry_handler,
            )

            try:
                df = runner.run(df, meta)
                self._run_dq_checkpoint(df, stage_cfg, meta)
                logger.info("Stage '%s' completed: %s", sname, meta.stage_results[sname])
            except Exception:
                logger.error("Pipeline halted at stage '%s'", sname)
                pipeline_failed = True
                break

        self._persist_lineage(meta)
        self._write_run_metadata(meta)
        logger.info("=== Pipeline '%s' finished | batch=%s ===", name, meta.batch_id)
        if pipeline_failed:
            raise RuntimeError(f"Pipeline '{name}' failed. See pipeline_runs for batch={meta.batch_id}.")
        return meta

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_spark_session(self) -> SparkSession:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        java_home = os.environ.get("JAVA_HOME")
        homebrew_java_home = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
        if not java_home and homebrew_java_home.exists():
            java_home = str(homebrew_java_home)
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = f"{Path(java_home) / 'bin'}{os.pathsep}{os.environ['PATH']}"

        java_path = str(Path(java_home) / "bin" / "java") if java_home else shutil.which("java")
        if not java_path:
            raise RuntimeError(
                "PySpark requires Java, but no Java runtime was found. "
                "Install OpenJDK 17 and set JAVA_HOME before running the pipeline."
            )
        try:
            subprocess.run(
                [java_path, "-version"],
                capture_output=True,
                check=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise RuntimeError(
                "PySpark requires a working Java runtime, but `java -version` failed. "
                "Install OpenJDK 17 and set JAVA_HOME before running the pipeline."
            ) from exc
        return (
            SparkSession.builder
            .appName("FinancialLineagePipeline")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate()
        )

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path) as f:
            config = yaml.safe_load(f)
        if not config:
            raise ValueError(f"Config file is empty: {self.config_path}")
        return config

    def _build_dq_engine(self):
        dq_config = self.config.get("dq", {})
        if dq_config.get("enabled", True) is False:
            return None
        try:
            from dq.rules_engine import DQRulesEngine
            return DQRulesEngine(
                config_path=dq_config.get("rules_config", "dq/rules_config.yaml"),
                duckdb_path=self.duckdb_path,
            )
        except Exception as exc:
            logger.warning("DQ engine disabled because initialization failed: %s", exc)
            return None

    def _read_input(self, input_path: str) -> DataFrame:
        p = Path(input_path)
        if not p.exists():
            raise FileNotFoundError(f"Input file does not exist: {p}")
        if p.suffix == ".parquet":
            return self.spark.read.parquet(str(p))
        elif p.suffix == ".csv":
            return (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(str(p))
            )
        else:
            raise ValueError(f"Unsupported input format: {p.suffix}")

    def _instantiate_transforms(
        self,
        transform_names: List[str],
        tracker: LineageTracker,
    ) -> List[BaseTransform]:
        """
        Resolves transform names to classes via the registry.
        Import here to avoid circular imports.
        """
        from etl.transforms.registry import TRANSFORM_REGISTRY, _load_all_transforms
        _load_all_transforms()
        instances = []
        for name in transform_names:
            cls = TRANSFORM_REGISTRY.get(name)
            if cls is None:
                raise ValueError(f"Transform '{name}' not found in registry. "
                                 f"Available: {list(TRANSFORM_REGISTRY.keys())}")
            instances.append(cls(config=self.config, dlq=self.dlq, tracker=tracker))
        return instances

    def _run_dq_checkpoint(
        self,
        df: DataFrame,
        stage_cfg: Dict[str, Any],
        meta: BatchMeta,
    ) -> None:
        checkpoint = stage_cfg.get("checkpoint")
        if not checkpoint or self.dq_engine is None:
            return
        result = self.dq_engine.run_checkpoint(
            df=df,
            checkpoint=checkpoint,
            batch_id=meta.batch_id,
        )
        meta.dq_results[checkpoint] = result

    def _persist_lineage(self, meta: BatchMeta) -> None:
        """Persist collected lineage without failing the pipeline if storage is unavailable."""
        if self.tracker.edge_count() == 0:
            logger.info("No lineage edges emitted for batch=%s", meta.batch_id)
            return
        self.tracker.save_to_duckdb(self.duckdb_path, meta.batch_id)

    def _write_run_metadata(self, meta: BatchMeta) -> None:
        """Persist run metadata to DuckDB for dashboard consumption."""
        try:
            import duckdb
            con = duckdb.connect(self.duckdb_path)
            con.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    batch_id        VARCHAR,
                    pipeline_name   VARCHAR,
                    started_at      TIMESTAMP,
                    stage_name      VARCHAR,
                    status          VARCHAR,
                    records_in      INTEGER,
                    records_out     INTEGER,
                    records_dead    INTEGER,
                    duration_sec    DOUBLE,
                    error           VARCHAR
                )
            """)
            rows = [
                (
                    meta.batch_id,
                    meta.pipeline_name,
                    meta.started_at,
                    r.stage_name,
                    r.status,
                    r.records_in,
                    r.records_out,
                    r.records_dead,
                    r.duration_sec,
                    r.error,
                )
                for r in meta.stage_results.values()
            ]
            if rows:
                con.executemany(
                    "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?)", rows
                )
            con.close()
            logger.info("Run metadata written to DuckDB: %d stage rows", len(rows))
        except Exception as exc:
            logger.warning("Could not persist run metadata: %s", exc)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Financial Lineage Pipeline runner")
    parser.add_argument("--config", default="config/pipeline_config.yaml")
    parser.add_argument("--input", required=True, help="Path to input file")
    parser.add_argument("--stage", default=None, help="Run a single stage only")
    args = parser.parse_args()

    try:
        pipeline = Pipeline(config_path=args.config)
        meta = pipeline.run(input_path=args.input, stage_filter=args.stage)
    except (FileNotFoundError, RuntimeError) as exc:
        parser.exit(1, f"error: {exc}\n")

    print("\n=== Run Summary ===")
    for stage_name, result in meta.stage_results.items():
        print(f"  {stage_name:20s} | {result.status:8s} | "
              f"in={result.records_in:,} out={result.records_out:,} "
              f"dead={result.records_dead:,} | {result.duration_sec:.2f}s")