# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate synthetic data
# MAGIC
# MAGIC Creates the schema and a Unity Catalog volume, then writes synthetic CSV extracts
# MAGIC (patients, clinicians, referrals, contacts) into the volume's landing area.
# MAGIC
# MAGIC The data is fake: NHS numbers are in the 999 test range, and deliberate errors are
# MAGIC injected so the silver layer has real data quality problems to catch.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nhs_lakehouse")
dbutils.widgets.text("patients", "2000")
dbutils.widgets.dropdown("regenerate", "false", ["true", "false"])

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.config import LakehouseConfig
from lakehouse.generator import generate, write_all

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume}")

# COMMAND ----------

# Keep existing landing files unless asked to rebuild, so re-running the job is repeatable
# and any simulated patient changes (notebook 04) are not overwritten.
already_there = os.path.exists(f"{cfg.landing_path}/contacts/contacts.csv")

if already_there and dbutils.widgets.get("regenerate") != "true":
    print("Landing files already exist, keeping them. Set regenerate = true to rebuild.")
else:
    tables = generate(n_patients=int(dbutils.widgets.get("patients")))
    for name, path in write_all(tables, cfg.landing_path).items():
        print(f"{name:<11} {len(tables[name]):>7,} rows -> {path}")
