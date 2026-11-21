# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingest raw extracts
# MAGIC
# MAGIC Loads every CSV in the landing area into a bronze Delta table **as it is**
# MAGIC (all columns as text), adding the source file, ingestion time and run ID.
# MAGIC
# MAGIC Completeness check: the number of rows loaded must equal the number of data
# MAGIC lines in the source files, or the step fails.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nhs_lakehouse")
dbutils.widgets.text("run_id", "")

# COMMAND ----------

import os
import sys
import uuid

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from lakehouse.audit import audit_row, write_audit
from lakehouse.config import ENTITIES, LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
print(f"run_id = {run_id}")

# COMMAND ----------


def source_line_count(folder: str) -> int:
    """Data lines (excluding headers) across every CSV file under a landing folder."""
    total = 0
    for root, _, files in os.walk(folder):
        for name in files:
            if name.endswith(".csv"):
                with open(os.path.join(root, name), encoding="utf-8") as f:
                    total += max(sum(1 for _ in f) - 1, 0)
    return total


audit = []
for entity in ENTITIES:
    folder = f"{cfg.landing_path}/{entity}"
    bronze = (
        spark.read.option("header", True)
        .option("recursiveFileLookup", True)
        .csv(folder)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    table = cfg.table(f"bronze_{entity}")
    bronze.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)

    loaded = spark.table(table).count()
    expected = source_line_count(folder)
    status = "success" if loaded == expected else "failed"
    audit.append(audit_row(run_id, "bronze", entity, expected, loaded,
                           status=status, message=f"source lines {expected}, loaded {loaded}"))
    print(f"{entity:<11} source {expected:>7,}  loaded {loaded:>7,}  {status}")

write_audit(spark, cfg.table("audit_log"), audit)

failed = [row[2] for row in audit if row[6] != "success"]
if failed:
    raise ValueError(f"Bronze completeness check failed for: {', '.join(failed)}")
