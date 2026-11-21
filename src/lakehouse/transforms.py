"""Gold-layer transformations (PySpark): the star schema and Type 2 history.

Star schema
    fact_contact   one row per clinical contact (the grain)
    dim_patient    Type 2 slowly changing dimension (history of GP practice and postcode)
    dim_date       calendar plus NHS financial year (April to March)
    dim_service    the four clinical services
    dim_clinician  clinicians and their roles
"""

from __future__ import annotations

import operator
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

OPEN_END_DATE = "9999-12-31"
HISTORY_START_DATE = "1900-01-01"
PATIENT_TRACKED_COLUMNS = ["gp_practice", "postcode"]


# ---------------------------------------------------------------------------
# dim_patient (Type 2 slowly changing dimension)
# ---------------------------------------------------------------------------

def patient_dimension_source(master_patients: DataFrame) -> DataFrame:
    """Shape silver master patients into the columns dim_patient tracks."""
    return master_patients.select(
        F.col("nhs_clean").alias("nhs_number"),
        "patient_id",
        "date_of_birth",
        "gp_practice",
        "postcode",
        F.to_date("last_updated").alias("effective_date"),
    )


def scd2_changes(current_dim: DataFrame, incoming: DataFrame, key: str = "nhs_number",
                 tracked: list[str] | None = None) -> DataFrame:
    """Return incoming rows that are new, or whose tracked columns have changed.

    Adds change_type = 'new' | 'changed'. Unchanged rows are left out, so re-running
    the load with the same data changes nothing (the load is idempotent).
    """
    tracked = tracked or PATIENT_TRACKED_COLUMNS
    current = current_dim.filter(F.col("is_current")).select(
        key,
        F.lit(True).alias("_exists"),
        *[F.col(c).alias(f"_cur_{c}") for c in tracked],
    )
    joined = incoming.join(current, key, "left")
    changed = reduce(operator.or_, [~F.col(c).eqNullSafe(F.col(f"_cur_{c}")) for c in tracked])
    is_new = F.col("_exists").isNull()
    return (joined.filter(is_new | changed)
                  .withColumn("change_type", F.when(is_new, F.lit("new")).otherwise(F.lit("changed")))
                  .drop("_exists", *[f"_cur_{c}" for c in tracked]))


def scd2_new_rows(changes: DataFrame) -> DataFrame:
    """Turn change rows into new current dim_patient rows.

    New patients are valid from the start of history, so older contacts still match.
    Changed patients are valid from the date of the change.
    """
    valid_from = F.when(F.col("change_type") == "new", F.lit(HISTORY_START_DATE).cast("date")) \
                  .otherwise(F.col("effective_date"))
    return changes.select(
        F.xxhash64("nhs_number", valid_from).alias("patient_key"),
        "nhs_number",
        "patient_id",
        "date_of_birth",
        "gp_practice",
        "postcode",
        valid_from.alias("valid_from"),
        F.lit(OPEN_END_DATE).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )


# ---------------------------------------------------------------------------
# Other dimensions
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    """One row per day between start and end (inclusive), with the NHS financial year."""
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date"
    )
    fy_start = F.when(F.month("date") >= 4, F.year("date")).otherwise(F.year("date") - 1)
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.concat(fy_start.cast("string"), F.lit("/"),
                 F.substring((fy_start + 1).cast("string"), 3, 2)).alias("financial_year"),
    )


def build_dim_service(clean_referrals: DataFrame) -> DataFrame:
    return (clean_referrals.select(F.col("service").alias("service_name")).distinct()
                           .select(F.xxhash64("service_name").alias("service_key"), "service_name"))


def build_dim_clinician(clinicians: DataFrame) -> DataFrame:
    return clinicians.select(
        F.xxhash64(F.trim("clinician_id")).alias("clinician_key"),
        F.trim("clinician_id").alias("clinician_id"),
        F.trim("clinician_name").alias("clinician_name"),
        F.trim("role").alias("role"),
        F.trim("service").alias("home_service"),
    )


# ---------------------------------------------------------------------------
# fact_contact
# ---------------------------------------------------------------------------

def build_fact_contact(clean_contacts: DataFrame, dim_patient: DataFrame, dim_service: DataFrame,
                       dim_clinician: DataFrame) -> DataFrame:
    """One row per clean contact, with surrogate keys to every dimension.

    The patient join is point-in-time: each contact links to the version of the
    patient that was valid on the contact date, which is what Type 2 history is for.
    """
    c = clean_contacts.alias("c")
    p = dim_patient.alias("p")
    s = dim_service.alias("s")
    k = dim_clinician.alias("k")
    patient_match = (
        (F.col("c.nhs_number") == F.col("p.nhs_number"))
        & (F.col("c.contact_date") >= F.col("p.valid_from"))
        & (F.col("c.contact_date") <= F.col("p.valid_to"))
    )
    return (c.join(p, patient_match, "left")
             .join(s, F.col("c.service") == F.col("s.service_name"), "left")
             .join(k, F.col("c.clinician_id") == F.col("k.clinician_id"), "left")
             .select(
                 F.xxhash64(F.col("c.contact_id")).alias("contact_key"),
                 F.col("c.contact_id").alias("contact_id"),
                 F.col("p.patient_key").alias("patient_key"),
                 F.date_format(F.col("c.contact_date"), "yyyyMMdd").cast("int").alias("date_key"),
                 F.col("s.service_key").alias("service_key"),
                 F.col("k.clinician_key").alias("clinician_key"),
                 F.col("c.referral_id").alias("referral_id"),
                 F.col("c.contact_type").alias("contact_type"),
                 F.col("c.duration_minutes").alias("duration_minutes"),
             ))


def unmatched_keys(fact: DataFrame) -> dict[str, int]:
    """Count fact rows missing a dimension key. All should be zero before publishing."""
    key_cols = ["patient_key", "date_key", "service_key", "clinician_key"]
    row = fact.select(*[F.sum(F.col(k).isNull().cast("int")).alias(k) for k in key_cols]).first()
    return {k: int(row[k] or 0) for k in key_cols}
