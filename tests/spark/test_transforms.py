import pytest

pytest.importorskip("pyspark")

from datetime import date  # noqa: E402

from lakehouse import transforms as t  # noqa: E402


def test_scd2_changes_finds_only_new_and_changed(spark):
    current = spark.createDataFrame(
        [
            ("N1", "Y00001", "E1 4AB", True),
            ("N2", "Y00002", "E2 6CD", True),
            ("N2", "Y00009", "E2 6CD", False),   # older, already-closed version
        ],
        "nhs_number string, gp_practice string, postcode string, is_current boolean",
    )
    incoming = spark.createDataFrame(
        [
            ("N1", "P1", date(1980, 1, 1), "Y00001", "E1 4AB", date(2026, 11, 1)),   # unchanged
            ("N2", "P2", date(1975, 1, 1), "Y00005", "E2 6CD", date(2026, 11, 1)),   # moved practice
            ("N4", "P4", date(1990, 1, 1), "Y00003", "E3 1EF", date(2026, 11, 1)),   # new patient
        ],
        "nhs_number string, patient_id string, date_of_birth date, gp_practice string, "
        "postcode string, effective_date date",
    )
    changes = t.scd2_changes(current, incoming)
    assert {(r["nhs_number"], r["change_type"]) for r in changes.collect()} == {("N2", "changed"), ("N4", "new")}

    rows = {r["nhs_number"]: r for r in t.scd2_new_rows(changes).collect()}
    assert rows["N4"]["valid_from"] == date(1900, 1, 1)
    assert rows["N2"]["valid_from"] == date(2026, 11, 1)
    assert all(r["valid_to"] == date(9999, 12, 31) and r["is_current"] for r in rows.values())


def test_dim_date_has_nhs_financial_year(spark):
    rows = {r["date"]: r for r in t.build_dim_date(spark, "2025-03-30", "2025-04-02").collect()}
    assert len(rows) == 4
    assert rows[date(2025, 3, 31)]["financial_year"] == "2024/25"
    assert rows[date(2025, 4, 1)]["financial_year"] == "2025/26"
    assert rows[date(2025, 4, 1)]["date_key"] == 20250401
    assert rows[date(2025, 3, 30)]["is_weekend"]          # a Sunday
    assert not rows[date(2025, 3, 31)]["is_weekend"]      # a Monday


def test_fact_contact_joins_the_right_patient_version(spark):
    dim_patient = spark.createDataFrame(
        [
            (1, "N1", date(1900, 1, 1), date(2025, 6, 30)),
            (2, "N1", date(2025, 7, 1), date(9999, 12, 31)),
        ],
        "patient_key long, nhs_number string, valid_from date, valid_to date",
    )
    dim_service = spark.createDataFrame([(10, "Wound Care")], "service_key long, service_name string")
    dim_clinician = spark.createDataFrame([(20, "C001")], "clinician_key long, clinician_id string")
    contacts = spark.createDataFrame(
        [
            ("CT1", "N1", date(2025, 5, 1), "Wound Care", "C001", "R1", "Telephone", 30),
            ("CT2", "N1", date(2025, 8, 1), "Wound Care", "C001", "R1", "Video", 45),
        ],
        "contact_id string, nhs_number string, contact_date date, service string, clinician_id string, "
        "referral_id string, contact_type string, duration_minutes int",
    )
    fact = t.build_fact_contact(contacts, dim_patient, dim_service, dim_clinician)
    keys = {r["contact_id"]: r["patient_key"] for r in fact.collect()}
    assert keys == {"CT1": 1, "CT2": 2}
    assert t.unmatched_keys(fact) == {"patient_key": 0, "date_key": 0, "service_key": 0, "clinician_key": 0}
