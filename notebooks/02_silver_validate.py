# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, validate and de-duplicate
# MAGIC
# MAGIC 1. Standardise types and text (dates, integers, NHS numbers, postcodes).
# MAGIC 2. Run the data quality rules. Failing rows go to `exceptions` with the reason.
# MAGIC 3. De-duplicate patients on NHS number, keeping the latest record, and re-point
# MAGIC    referrals and contacts from old duplicate IDs to the surviving patient.
# MAGIC 4. Reconcile: clean rows + rejected rows must equal rows read.

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

from lakehouse import quality as q
from lakehouse.audit import audit_row, write_audit
from lakehouse.config import LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())


def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(cfg.table(name))
    return spark.table(cfg.table(name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patients

# COMMAND ----------

bronze_patients = spark.table(cfg.table("bronze_patients"))
patients = q.validate_patients(q.standardise_patients(bronze_patients))
valid_patients, bad_patients = q.split_by_issues(patients)
masters, xref, superseded = q.deduplicate_patients(valid_patients)

silver_patients = save(masters, "silver_patients")
silver_xref = save(xref, "silver_patient_xref")
save(q.to_exceptions(bad_patients, "patients", "patient_id", run_id)
      .unionByName(q.to_exceptions(superseded, "patients", "patient_id", run_id)),
     "stg_exceptions_patients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referrals

# COMMAND ----------

bronze_referrals = spark.table(cfg.table("bronze_referrals"))
referrals = q.validate_referrals(q.standardise_referrals(bronze_referrals), silver_xref)
clean_referrals, bad_referrals = q.split_by_issues(referrals)

silver_referrals = save(clean_referrals, "silver_referrals")
save(q.to_exceptions(bad_referrals, "referrals", "referral_id", run_id), "stg_exceptions_referrals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contacts

# COMMAND ----------

bronze_contacts = spark.table(cfg.table("bronze_contacts"))
contacts = q.validate_contacts(q.standardise_contacts(bronze_contacts), silver_referrals, silver_xref)
clean_contacts, bad_contacts = q.split_by_issues(contacts)

silver_contacts = save(clean_contacts, "silver_contacts")
save(q.to_exceptions(bad_contacts, "contacts", "contact_id", run_id), "stg_exceptions_contacts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exceptions, reconciliation and audit

# COMMAND ----------

exceptions = (spark.table(cfg.table("stg_exceptions_patients"))
              .unionByName(spark.table(cfg.table("stg_exceptions_referrals")))
              .unionByName(spark.table(cfg.table("stg_exceptions_contacts"))))

# Exceptions are kept as history across runs. Clear this run's rows first,
# so repairing a failed run never double-counts.
if spark.catalog.tableExists(cfg.table("exceptions")):
    spark.sql(f"DELETE FROM {cfg.table('exceptions')} WHERE run_id = '{run_id}'")
exceptions.write.mode("append").saveAsTable(cfg.table("exceptions"))

this_run = spark.table(cfg.table("exceptions")).filter(F.col("run_id") == run_id)
rejected = {row["source_table"]: row["count"] for row in this_run.groupBy("source_table").count().collect()}

checks = [
    ("patients", bronze_patients.count(), silver_patients.count(), rejected.get("patients", 0)),
    ("referrals", bronze_referrals.count(), silver_referrals.count(), rejected.get("referrals", 0)),
    ("contacts", bronze_contacts.count(), silver_contacts.count(), rejected.get("contacts", 0)),
]

audit, failed = [], []
for entity, read, loaded, bad in checks:
    status = "success" if loaded + bad == read else "failed"
    if status == "failed":
        failed.append(entity)
    audit.append(audit_row(run_id, "silver", entity, read, loaded, bad, status,
                           f"read {read} = loaded {loaded} + rejected {bad}"))
    print(f"{entity:<10} read {read:>7,}  loaded {loaded:>7,}  rejected {bad:>6,}  {status}")

write_audit(spark, cfg.table("audit_log"), audit)
if failed:
    raise ValueError(f"Silver reconciliation failed for: {', '.join(failed)}")

# COMMAND ----------

# Weekly exceptions list: one row per issue type, ready to send back to each service.
display(
    this_run.select("source_table", F.explode(F.split("issues", ";")).alias("issue"))
    .groupBy("source_table", "issue").count()
    .orderBy("source_table", F.desc("count"))
)
