import os
import sys

import pytest

HERE = os.path.dirname(__file__)
for sub in ("../spark_jobs", "../data_generator", "."):
    sys.path.insert(0, os.path.abspath(os.path.join(HERE, sub)))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[1]")
         .appName("unit-tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         # giống docker/spark/spark-defaults.conf (xem test_streaming_plan.py)
         .config("spark.sql.constraintPropagation.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def key():
    return b"0123456789abcdef0123456789abcdef0123456789abcdef"
