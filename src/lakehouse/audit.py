"""Audit logging: every pipeline step records what it read, loaded and rejected."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("step", StringType()),
    StructField("entity", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_loaded", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("status", StringType()),
    StructField("message", StringType()),
    StructField("logged_at", TimestampType()),
])


def audit_row(run_id: str, step: str, entity: str, rows_read: int, rows_loaded: int,
              rows_rejected: int = 0, status: str = "success", message: str = "") -> tuple:
    return (run_id, step, entity, int(rows_read), int(rows_loaded), int(rows_rejected),
            status, message, datetime.now(timezone.utc).replace(tzinfo=None))


def write_audit(spark: SparkSession, table_name: str, rows: list[tuple]) -> None:
    """Append audit rows to the audit Delta table (created on first write)."""
    spark.createDataFrame(rows, AUDIT_SCHEMA).write.mode("append").saveAsTable(table_name)
