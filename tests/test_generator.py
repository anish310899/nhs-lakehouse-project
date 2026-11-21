import csv
from collections import Counter
from datetime import date

from lakehouse.generator import generate, generate_patient_updates, write_all
from lakehouse.nhs import is_valid_nhs_number

AS_OF = date(2026, 11, 1)


def test_same_seed_gives_same_data():
    assert generate(200, seed=1, as_of=AS_OF) == generate(200, seed=1, as_of=AS_OF)


def test_tables_and_columns():
    tables = generate(200, seed=1, as_of=AS_OF)
    assert set(tables) == {"clinicians", "patients", "referrals", "contacts"}
    assert set(tables["contacts"][0]) == {
        "contact_id", "referral_id", "patient_id", "clinician_id",
        "contact_date", "contact_type", "duration_minutes", "last_modified",
    }


def test_valid_nhs_numbers_are_in_the_test_range():
    patients = generate(500, seed=2, as_of=AS_OF)["patients"]
    valid = [p["nhs_number"] for p in patients if is_valid_nhs_number(p["nhs_number"])]
    assert valid, "expected some valid NHS numbers"
    assert all(n.startswith("999") for n in valid)


def test_deliberate_errors_are_present():
    tables = generate(2000, seed=42, as_of=AS_OF)
    patients, contacts = tables["patients"], tables["contacts"]

    nhs_counts = Counter(p["nhs_number"] for p in patients if p["nhs_number"])
    assert any(count > 1 for count in nhs_counts.values()), "duplicate patients"
    assert any(p["nhs_number"] == "" for p in patients), "missing NHS numbers"
    assert any(p["nhs_number"] and not is_valid_nhs_number(p["nhs_number"]) for p in patients), \
        "invalid NHS numbers"

    contact_ids = Counter(c["contact_id"] for c in contacts)
    assert any(count > 1 for count in contact_ids.values()), "duplicate contacts"
    assert any(c["contact_date"] == "" for c in contacts), "missing contact dates"
    assert any(c["contact_date"] and date.fromisoformat(c["contact_date"]) > AS_OF for c in contacts), \
        "future contact dates"
    assert any(int(c["duration_minutes"]) <= 0 for c in contacts), "impossible durations"
    assert any(c["patient_id"] == "P999999" for c in contacts), "unknown patients"


def test_write_and_update(tmp_path):
    tables = generate(100, seed=3, as_of=AS_OF)
    paths = write_all(tables, str(tmp_path))
    with open(paths["patients"], newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == len(tables["patients"])

    updates = generate_patient_updates(paths["patients"], fraction=0.1, as_of=date(2026, 11, 20))
    assert updates
    originals = {r["patient_id"]: r for r in rows}
    for u in updates:
        before = originals[u["patient_id"]]
        assert (u["gp_practice"], u["postcode"]) != (before["gp_practice"], before["postcode"])
        assert u["last_updated"] == "2026-11-20T00:00:00"
