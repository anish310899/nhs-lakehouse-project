import pytest

pytest.importorskip("pyspark")

from datetime import datetime  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

from lakehouse import quality as q  # noqa: E402
from lakehouse.nhs import is_valid_nhs_number  # noqa: E402

AS_OF = F.lit("2026-01-31").cast("date")

CONTACT_COLUMNS = "contact_id string, referral_id string, patient_id string, clinician_id string, " \
                  "contact_date string, contact_type string, duration_minutes string, last_modified string"


def test_nhs_expression_matches_python_reference(spark):
    values = ["9434765919", "9434765918", "943476591", "94347659190", "94347659A9", "", None]
    df = spark.createDataFrame([(v,) for v in values], "nhs_clean string")
    result = {row["nhs_clean"]: row["ok"]
              for row in df.select("nhs_clean", q.nhs_number_is_valid("nhs_clean").alias("ok")).collect()}
    for value in values:
        assert result[value] == is_valid_nhs_number(value), value


def test_validate_patients_flags_bad_nhs_numbers(spark):
    bronze = spark.createDataFrame(
        [
            ("P1", "943 476 5919", "amara", "khan", "1980-05-01", "e1 4ab", "y00001", "2025-01-01T00:00:00"),
            ("P2", "9434765918", "ben", "smith", "1975-02-03", "E2 6CD", "Y00002", "2025-01-01T00:00:00"),
            ("P3", "", "chloe", "patel", "1990-07-09", "E3 1EF", "Y00003", "2025-01-01T00:00:00"),
            ("P4", "9434765919", "dev", "jones", "2099-01-01", "E5 2GH", "Y00004", "2025-01-01T00:00:00"),
        ],
        "patient_id string, nhs_number string, first_name string, last_name string, "
        "date_of_birth string, postcode string, gp_practice string, last_updated string",
    )
    result = {r["patient_id"]: r["issues"]
              for r in q.validate_patients(q.standardise_patients(bronze), AS_OF).collect()}
    assert result == {
        "P1": "",
        "P2": "invalid_nhs_number",
        "P3": "missing_nhs_number",
        "P4": "future_date_of_birth",
    }


def test_deduplicate_keeps_latest_and_maps_old_ids(spark):
    patients = spark.createDataFrame(
        [
            ("P1", "9434765919", "E1 4AB", datetime(2025, 1, 1)),
            ("P9", "9434765919", "E8 2XY", datetime(2026, 1, 1)),   # same person, re-registered later
            ("P2", "9990000018", "E2 6CD", datetime(2025, 6, 1)),
        ],
        "patient_id string, nhs_clean string, postcode string, last_updated timestamp",
    )
    masters, xref, superseded = q.deduplicate_patients(patients)

    assert {r["patient_id"] for r in masters.collect()} == {"P9", "P2"}
    assert {(r["patient_id"], r["master_patient_id"]) for r in xref.collect()} == {
        ("P1", "P9"), ("P9", "P9"), ("P2", "P2"),
    }
    assert [r["patient_id"] for r in superseded.collect()] == ["P1"]


def test_validate_contacts_flags_each_rule(spark):
    referrals = spark.createDataFrame(
        [("R1", "2026-01-01", "Wound Care")],
        "referral_id string, referral_date string, service string",
    ).withColumn("referral_date", F.to_date("referral_date"))
    xref = spark.createDataFrame([("P1", "P1", "9434765919")],
                                 "patient_id string, master_patient_id string, nhs_number string")

    def contact(cid, date="2026-01-10", referral="R1", patient="P1", duration="30"):
        return (cid, referral, patient, "C001", date, "Telephone", duration, "2026-01-10T00:00:00")

    bronze = spark.createDataFrame(
        [
            contact("CT1"),
            contact("CT2", date="2026-03-01"),
            contact("CT3", date="2025-12-01"),
            contact("CT4", date=""),
            contact("CT5", patient="P9"),
            contact("CT6"),
            contact("CT6"),
            contact("CT7", duration="0"),
            contact("CT8", referral="R9"),
        ],
        CONTACT_COLUMNS,
    )
    rows = q.validate_contacts(q.standardise_contacts(bronze), referrals, xref, AS_OF).collect()
    issues = {}
    for r in rows:
        issues.setdefault(r["contact_id"], []).append(r["issues"])

    assert issues["CT1"] == [""]
    assert issues["CT2"] == ["future_contact_date"]
    assert issues["CT3"] == ["contact_before_referral"]
    assert issues["CT4"] == ["missing_contact_date"]
    assert issues["CT5"] == ["unknown_patient"]
    assert sorted(issues["CT6"]) == ["", "duplicate_contact"]
    assert issues["CT7"] == ["invalid_duration"]
    assert issues["CT8"] == ["unknown_referral"]


def test_split_and_exceptions_shape(spark):
    df = spark.createDataFrame([("A", ""), ("B", "missing_nhs_number")], "patient_id string, issues string")
    clean, rejected = q.split_by_issues(df)
    assert [r["patient_id"] for r in clean.collect()] == ["A"]
    assert "issues" not in clean.columns

    exceptions = q.to_exceptions(rejected, "patients", "patient_id", "run-1").collect()
    assert len(exceptions) == 1
    assert exceptions[0]["record_id"] == "B"
    assert exceptions[0]["issues"] == "missing_nhs_number"
    assert exceptions[0]["run_id"] == "run-1"
