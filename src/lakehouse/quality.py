"""Silver-layer data quality rules (PySpark).

Every rule adds a named issue to an `issues` column. Rows with no issues carry on
to the clean silver tables; rows with any issue are quarantined in the
exceptions table with the reason, instead of silently breaking a report.

Only built-in Spark functions are used (no Python UDFs), so the rules run fast
and work on Databricks serverless compute.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_date(col_name: str) -> Column:
    """Parse a yyyy-MM-dd string to a date; bad or empty values become null."""
    return F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd')").cast("date")


def safe_timestamp(col_name: str) -> Column:
    """Parse an ISO timestamp string; bad or empty values become null."""
    return F.expr(f"try_to_timestamp({col_name})")


def safe_int(col_name: str) -> Column:
    """Cast a string to an integer; bad values become null instead of failing."""
    return F.expr(f"try_cast({col_name} AS INT)")


def nhs_number_is_valid(col_name: str = "nhs_clean") -> Column:
    """Modulus 11 check of a cleaned (no spaces) NHS number, as a Spark expression.

    Mirrors lakehouse.nhs.is_valid_nhs_number, which the unit tests use as the reference.
    """
    digits = [F.expr(f"try_cast(substring({col_name}, {i}, 1) AS INT)") for i in range(1, 11)]
    weighted = sum(digit * (11 - i) for i, digit in enumerate(digits[:9], start=1))
    remainder = weighted % 11
    check = F.when(remainder == 0, F.lit(0)).otherwise(F.lit(11) - remainder)
    is_valid = F.col(col_name).rlike(r"^\d{10}$") & (check != 10) & (check == digits[9])
    return F.coalesce(is_valid, F.lit(False))


def issues_column(rules: list[tuple[str, Column]]) -> Column:
    """Combine (issue_name, condition) pairs into one ';'-separated issues string.

    A rule whose condition is null (for example because a date is missing) does not
    fire; a separate 'missing_...' rule catches that case.
    """
    return F.concat_ws(";", *[F.when(condition, F.lit(name)) for name, condition in rules])


def split_by_issues(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean rows without the issues column, rows that have issues)."""
    clean = df.filter(F.col("issues") == "").drop("issues")
    rejected = df.filter(F.col("issues") != "")
    return clean, rejected


def to_exceptions(rejected: DataFrame, source_table: str, id_col: str, run_id: str) -> DataFrame:
    """Standard exceptions shape: one row per rejected record, with the raw record as JSON."""
    record_cols = [c for c in rejected.columns if c != "issues" and not c.startswith("_")]
    return rejected.select(
        F.lit(run_id).alias("run_id"),
        F.lit(source_table).alias("source_table"),
        F.col(id_col).cast("string").alias("record_id"),
        F.col("issues"),
        F.to_json(F.struct(*record_cols)).alias("record_json"),
        F.current_timestamp().alias("detected_at"),
    )


# ---------------------------------------------------------------------------
# Patients
# ---------------------------------------------------------------------------

def standardise_patients(bronze: DataFrame) -> DataFrame:
    """Trim text, remove spaces from NHS numbers, upper-case postcodes, type the dates."""
    return bronze.select(
        F.trim("patient_id").alias("patient_id"),
        F.regexp_replace(F.col("nhs_number"), r"\s", "").alias("nhs_clean"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        safe_date("date_of_birth").alias("date_of_birth"),
        F.upper(F.trim("postcode")).alias("postcode"),
        F.upper(F.trim("gp_practice")).alias("gp_practice"),
        safe_timestamp("last_updated").alias("last_updated"),
    )


def validate_patients(patients: DataFrame, as_of: Column | None = None) -> DataFrame:
    """Add an issues column to standardised patients."""
    as_of = as_of if as_of is not None else F.current_date()
    rules = [
        ("missing_patient_id", F.col("patient_id").isNull()),
        ("missing_nhs_number", F.col("nhs_clean").isNull() | (F.col("nhs_clean") == "")),
        ("invalid_nhs_number", F.col("nhs_clean").isNotNull() & (F.col("nhs_clean") != "")
                               & ~nhs_number_is_valid("nhs_clean")),
        ("missing_date_of_birth", F.col("date_of_birth").isNull()),
        ("future_date_of_birth", F.col("date_of_birth") > as_of),
    ]
    return patients.withColumn("issues", issues_column(rules))


def deduplicate_patients(valid_patients: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Keep the most recently updated record for each NHS number.

    Returns:
        masters:    one row per NHS number (the surviving record)
        xref:       patient_id -> master_patient_id and nhs_number, so contacts recorded
                    against an old duplicate ID are re-pointed to the surviving patient
        superseded: the older duplicate records, kept for audit
    """
    w = Window.partitionBy("nhs_clean").orderBy(F.col("last_updated").desc_nulls_last(),
                                               F.col("patient_id").desc())
    ranked = valid_patients.withColumn("_rn", F.row_number().over(w))
    masters = ranked.filter(F.col("_rn") == 1).drop("_rn")
    master_ids = masters.select("nhs_clean", F.col("patient_id").alias("master_patient_id"))
    xref = (ranked.join(master_ids, "nhs_clean")
                  .select("patient_id", "master_patient_id", F.col("nhs_clean").alias("nhs_number"))
                  .distinct())
    superseded = (ranked.filter(F.col("_rn") > 1)
                        .drop("_rn")
                        .withColumn("issues", F.lit("duplicate_superseded")))
    return masters, xref, superseded


# ---------------------------------------------------------------------------
# Referrals
# ---------------------------------------------------------------------------

def standardise_referrals(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.trim("referral_id").alias("referral_id"),
        F.trim("patient_id").alias("patient_id"),
        F.trim("service").alias("service"),
        F.trim("referral_source").alias("referral_source"),
        safe_date("referral_date").alias("referral_date"),
    )


def validate_referrals(referrals: DataFrame, xref: DataFrame, as_of: Column | None = None) -> DataFrame:
    """Add master_patient_id and an issues column to standardised referrals."""
    as_of = as_of if as_of is not None else F.current_date()
    joined = referrals.join(xref, "patient_id", "left")
    rules = [
        ("missing_referral_id", F.col("referral_id").isNull()),
        ("missing_referral_date", F.col("referral_date").isNull()),
        ("future_referral_date", F.col("referral_date") > as_of),
        ("unknown_patient", F.col("master_patient_id").isNull()),
        ("missing_service", F.col("service").isNull()),
    ]
    return joined.withColumn("issues", issues_column(rules))


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

def standardise_contacts(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.trim("contact_id").alias("contact_id"),
        F.trim("referral_id").alias("referral_id"),
        F.trim("patient_id").alias("patient_id"),
        F.trim("clinician_id").alias("clinician_id"),
        safe_date("contact_date").alias("contact_date"),
        F.trim("contact_type").alias("contact_type"),
        safe_int("duration_minutes").alias("duration_minutes"),
        safe_timestamp("last_modified").alias("last_modified"),
    )


def validate_contacts(contacts: DataFrame, clean_referrals: DataFrame, xref: DataFrame,
                      as_of: Column | None = None) -> DataFrame:
    """Add referral details, master_patient_id and an issues column to standardised contacts.

    `clean_referrals` must already have passed validate_referrals, so a contact whose
    referral was quarantined is flagged as unknown_referral too.
    """
    as_of = as_of if as_of is not None else F.current_date()

    w = Window.partitionBy("contact_id").orderBy(F.col("last_modified").desc_nulls_last())
    ranked = contacts.withColumn("_rn", F.row_number().over(w))

    referral_info = clean_referrals.select(
        "referral_id", "referral_date", "service",
        F.lit(True).alias("_referral_found"),
    )
    joined = (ranked.join(referral_info, "referral_id", "left")
                    .join(xref, "patient_id", "left"))

    rules = [
        ("missing_contact_id", F.col("contact_id").isNull()),
        ("duplicate_contact", F.col("_rn") > 1),
        ("missing_contact_date", F.col("contact_date").isNull()),
        ("future_contact_date", F.col("contact_date") > as_of),
        ("unknown_referral", F.col("_referral_found").isNull()),
        ("contact_before_referral", F.col("contact_date") < F.col("referral_date")),
        ("unknown_patient", F.col("master_patient_id").isNull()),
        ("invalid_duration", F.col("duration_minutes").isNull() | (F.col("duration_minutes") <= 0)),
    ]
    return joined.withColumn("issues", issues_column(rules)).drop("_rn", "_referral_found")
