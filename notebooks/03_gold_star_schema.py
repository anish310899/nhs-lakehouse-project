# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: star schema with Type 2 patient history
# MAGIC
# MAGIC - `dim_patient` keeps history of GP practice and postcode (Type 2 slowly changing dimension)
# MAGIC - `dim_date`, `dim_service`, `dim_clinician`
# MAGIC - `fact_contact`, one row per clinical contact, joined point-in-time to `dim_patient`
# MAGIC - `vw_contact_reporting`, a flat view for Power BI
# MAGIC
# MAGIC Reconciliation: fact rows must equal clean silver contacts, and every fact row
# MAGIC must find all of its dimension keys, or the step fails.

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

from lakehouse import transforms as t
from lakehouse.audit import audit_row, write_audit
from lakehouse.config import LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
dim_patient = cfg.table("dim_patient")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_patient (Type 2)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dim_patient} (
    patient_key   BIGINT,
    nhs_number    STRING,
    patient_id    STRING,
    date_of_birth DATE,
    gp_practice   STRING,
    postcode      STRING,
    valid_from    DATE,
    valid_to      DATE,
    is_current    BOOLEAN
)
""")

incoming = t.patient_dimension_source(spark.table(cfg.table("silver_patients")))
changes = t.scd2_changes(spark.table(dim_patient), incoming)

# Materialise the changes before touching dim_patient. Spark is lazy, so without this
# the comparison would be re-evaluated against the table after the MERGE below.
changes.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table("stg_patient_changes"))
staged = spark.table(cfg.table("stg_patient_changes"))

# Step 1: close the current version of every changed patient.
spark.sql(f"""
MERGE INTO {dim_patient} AS t
USING (SELECT nhs_number, effective_date
       FROM {cfg.table('stg_patient_changes')}
       WHERE change_type = 'changed') AS s
ON t.nhs_number = s.nhs_number AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.is_current = false,
    t.valid_to   = date_sub(s.effective_date, 1)
""")

# Step 2: insert a new current version for every new or changed patient.
t.scd2_new_rows(staged).write.mode("append").saveAsTable(dim_patient)

summary = {row["change_type"]: row["count"] for row in staged.groupBy("change_type").count().collect()}
print(f"dim_patient: {summary.get('new', 0):,} new, {summary.get('changed', 0):,} changed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other dimensions

# COMMAND ----------

silver_contacts = spark.table(cfg.table("silver_contacts"))

t.build_dim_service(spark.table(cfg.table("silver_referrals"))) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table("dim_service"))

t.build_dim_clinician(spark.table(cfg.table("bronze_clinicians"))) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table("dim_clinician"))

bounds = silver_contacts.agg(F.min("contact_date").alias("lo"), F.max("contact_date").alias("hi")).first()
t.build_dim_date(spark, f"{bounds['lo'].year}-01-01", f"{bounds['hi'].year}-12-31") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table("dim_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_contact and reconciliation

# COMMAND ----------

fact = t.build_fact_contact(
    silver_contacts,
    spark.table(dim_patient),
    spark.table(cfg.table("dim_service")),
    spark.table(cfg.table("dim_clinician")),
)
fact.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table("fact_contact"))

fact_table = spark.table(cfg.table("fact_contact"))
expected, loaded = silver_contacts.count(), fact_table.count()
missing_keys = t.unmatched_keys(fact_table)
problems = [f"{k}: {v}" for k, v in missing_keys.items() if v]
status = "success" if loaded == expected and not problems else "failed"
message = f"silver {expected}, fact {loaded}; missing keys: {', '.join(problems) or 'none'}"

write_audit(spark, cfg.table("audit_log"), [audit_row(run_id, "gold", "fact_contact", expected, loaded,
                                                      status=status, message=message)])
print(message)
if status == "failed":
    raise ValueError(f"Gold reconciliation failed: {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reporting view for Power BI

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table('vw_contact_reporting')} AS
SELECT f.contact_id,
       d.date, d.month_name, d.year, d.financial_year,
       s.service_name,
       k.clinician_name, k.role,
       p.nhs_number, p.gp_practice,
       f.contact_type, f.duration_minutes
FROM {cfg.table('fact_contact')} f
JOIN {cfg.table('dim_date')}      d ON f.date_key = d.date_key
JOIN {cfg.table('dim_service')}   s ON f.service_key = s.service_key
JOIN {cfg.table('dim_clinician')} k ON f.clinician_key = k.clinician_key
JOIN {dim_patient}                p ON f.patient_key = p.patient_key
""")
display(spark.table(cfg.table("vw_contact_reporting")).limit(20))
