# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Simulate patient changes (Type 2 demo)
# MAGIC
# MAGIC Writes a second patients extract in which about 5% of patients have moved
# MAGIC GP practice or house. Re-run notebooks 01 to 03 (or the job) afterwards:
# MAGIC silver picks up the newer records, and gold closes the old `dim_patient`
# MAGIC versions and adds new current ones, so past contacts keep reporting
# MAGIC under the practice the patient had at the time.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nhs_lakehouse")
dbutils.widgets.text("fraction", "0.05")

# COMMAND ----------

import os
import sys
from datetime import date

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.config import LakehouseConfig
from lakehouse.generator import generate_patient_updates, write_csv

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))

updates = generate_patient_updates(
    f"{cfg.landing_path}/patients/patients.csv",
    fraction=float(dbutils.widgets.get("fraction")),
)
path = f"{cfg.landing_path}/patients/patients_update_{date.today():%Y%m%d}.csv"
write_csv(updates, path)
print(f"{len(updates):,} patient changes written to {path}")
print("Now re-run notebooks 01, 02 and 03, then run the cell below.")

# COMMAND ----------

# After re-running 01-03: patients with more than one version in dim_patient.
display(spark.sql(f"""
SELECT nhs_number, patient_id, gp_practice, postcode, valid_from, valid_to, is_current
FROM {cfg.table('dim_patient')}
WHERE nhs_number IN (
    SELECT nhs_number FROM {cfg.table('dim_patient')} GROUP BY nhs_number HAVING COUNT(*) > 1
)
ORDER BY nhs_number, valid_from
"""))
