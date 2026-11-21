"""Spark tests run on a small local SparkSession.

Each Spark test module calls pytest.importorskip("pyspark"), so these tests are
skipped when PySpark isn't installed and the pure-Python tests still run anywhere.
GitHub Actions installs PySpark and Java, so every test runs in CI.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nhs-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
