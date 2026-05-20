from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture(scope="session")
def spark():
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Java runtime is required for PySpark tests")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("lineage-pipeline-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"PySpark session could not start: {exc}")
    yield session
    session.stop()

