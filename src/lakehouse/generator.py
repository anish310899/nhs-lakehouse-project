"""Synthetic data generator for the NHS Community Health Lakehouse.

Creates fake patients, clinicians, referrals and clinical contacts as CSV files,
with deliberate data quality problems so the silver layer has something to catch:

- duplicate patients (same NHS number, different internal patient ID)
- missing and invalid NHS numbers
- duplicate contact rows
- contacts dated in the future or before their referral
- missing contact dates, zero or negative durations
- contacts pointing at patients or referrals that don't exist

Only the Python standard library is used, so it runs anywhere:
locally, in GitHub Actions, or inside a Databricks notebook.
All NHS numbers are in the 999 test range and no real patient data is used.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

from lakehouse.nhs import nhs_check_digit

SERVICES = ["Wound Care", "Lymphoedema", "Podiatry", "Tissue Viability"]
REFERRAL_SOURCES = ["GP", "District Nurse", "Hospital", "Self-referral"]
CONTACT_TYPES = ["Face to face", "Telephone", "Video"]
ROLES = ["Specialist Nurse", "Clinical Lead", "Podiatrist", "Healthcare Assistant"]
GP_PRACTICES = [f"Y{n:05d}" for n in range(1, 21)]  # synthetic practice codes
FIRST_NAMES = [
    "Amara", "Ben", "Chloe", "Dev", "Ella", "Farah", "George", "Hana", "Isaac",
    "Jaya", "Kofi", "Leah", "Mohammed", "Nina", "Oscar", "Priya", "Ravi",
    "Sara", "Tom", "Uma", "Victor", "Wei", "Yusuf", "Zara",
]
LAST_NAMES = [
    "Ahmed", "Brown", "Chen", "Davies", "Evans", "Fernandes", "Green", "Hussain",
    "Iqbal", "Jones", "Khan", "Lewis", "Miah", "Nowak", "Okafor", "Patel",
    "Quinn", "Rahman", "Smith", "Taylor", "Uddin", "Walker", "Young",
]
POSTCODE_DISTRICTS = ["E1", "E2", "E3", "E5", "E8", "E9", "E14", "E15", "EC1", "N1"]


def make_nhs_number(rng: random.Random, used: set[str]) -> str:
    """Return a unique, valid NHS number in the 999 test range."""
    while True:
        first_nine = "999" + f"{rng.randint(0, 999_999):06d}"
        check = nhs_check_digit(first_nine)
        if check is None:
            continue
        number = first_nine + str(check)
        if number not in used:
            used.add(number)
            return number


def make_invalid_nhs_number(rng: random.Random) -> str:
    """Return a number that fails validation (wrong length or wrong check digit)."""
    if rng.random() < 0.5:
        return "999" + f"{rng.randint(0, 999_999):06d}"  # only nine digits
    while True:
        first_nine = "999" + f"{rng.randint(0, 999_999):06d}"
        check = nhs_check_digit(first_nine)
        if check is not None:
            return first_nine + str((check + 1) % 10)  # deliberately wrong digit


def random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def postcode(rng: random.Random) -> str:
    letters = "ABDEFGHJLNPQRSTUWXYZ"
    return f"{rng.choice(POSTCODE_DISTRICTS)} {rng.randint(1, 9)}{rng.choice(letters)}{rng.choice(letters)}"


def iso(value: date | datetime | None) -> str:
    return "" if value is None else value.isoformat()


def generate(n_patients: int = 2000, seed: int = 42, as_of: date | None = None) -> dict[str, list[dict]]:
    """Generate all tables as lists of dictionaries (one per CSV row)."""
    rng = random.Random(seed)
    as_of = as_of or date.today()
    used_nhs: set[str] = set()

    # Clinicians
    clinicians = []
    for i in range(1, 41):
        clinicians.append({
            "clinician_id": f"C{i:03d}",
            "clinician_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "role": rng.choice(ROLES),
            "service": SERVICES[(i - 1) % len(SERVICES)],
        })

    # Patients
    patients = []
    for i in range(1, n_patients + 1):
        roll = rng.random()
        if roll < 0.01:
            nhs = ""  # missing NHS number
        elif roll < 0.02:
            nhs = make_invalid_nhs_number(rng)
        else:
            nhs = make_nhs_number(rng, used_nhs)
        patients.append({
            "patient_id": f"P{i:06d}",
            "nhs_number": nhs,
            "first_name": rng.choice(FIRST_NAMES),
            "last_name": rng.choice(LAST_NAMES),
            "date_of_birth": iso(random_date(rng, date(1930, 1, 1), date(2005, 12, 31))),
            "postcode": postcode(rng),
            "gp_practice": rng.choice(GP_PRACTICES),
            "last_updated": iso(datetime.combine(
                random_date(rng, as_of - timedelta(days=720), as_of - timedelta(days=200)),
                datetime.min.time(),
            )),
        })

    # Duplicate patients: same person re-registered under a new patient ID
    next_id = n_patients + 1
    originals = [p for p in patients if p["nhs_number"] and len(p["nhs_number"]) == 10]
    for original in rng.sample(originals, k=max(1, int(n_patients * 0.03))):
        duplicate = dict(original)
        duplicate["patient_id"] = f"P{next_id:06d}"
        duplicate["last_updated"] = iso(datetime.combine(
            random_date(rng, as_of - timedelta(days=150), as_of - timedelta(days=30)),
            datetime.min.time(),
        ))
        if rng.random() < 0.5:
            duplicate["postcode"] = postcode(rng)  # moved house since first registration
        patients.append(duplicate)
        next_id += 1

    # Referrals: one to three per patient
    referrals = []
    referral_no = 1
    for p in patients:
        for _ in range(rng.choice([1, 1, 2, 3])):
            referrals.append({
                "referral_id": f"R{referral_no:07d}",
                "patient_id": p["patient_id"],
                "service": rng.choice(SERVICES),
                "referral_source": rng.choice(REFERRAL_SOURCES),
                "referral_date": iso(random_date(rng, as_of - timedelta(days=540), as_of - timedelta(days=60))),
            })
            referral_no += 1

    # Contacts: several per referral, with deliberate errors
    service_clinicians = {s: [c["clinician_id"] for c in clinicians if c["service"] == s] for s in SERVICES}
    contacts = []
    contact_no = 1
    for r in referrals:
        referral_date = date.fromisoformat(r["referral_date"])
        for _ in range(rng.randint(2, 12)):
            contact_date = random_date(rng, referral_date, as_of - timedelta(days=1))
            duration = rng.choice([15, 20, 30, 30, 45, 60])
            patient_id = r["patient_id"]
            referral_id = r["referral_id"]
            roll = rng.random()
            if roll < 0.005:
                contact_date = as_of + timedelta(days=rng.randint(10, 60))  # future date
            elif roll < 0.010:
                contact_date = referral_date - timedelta(days=rng.randint(1, 30))  # before referral
            elif roll < 0.015:
                contact_date = None  # missing date
            elif roll < 0.020:
                duration = rng.choice([0, -15])  # impossible duration
            elif roll < 0.023:
                patient_id = "P999999"  # unknown patient
            elif roll < 0.026:
                referral_id = "R9999999"  # unknown referral
            contacts.append({
                "contact_id": f"CT{contact_no:08d}",
                "referral_id": referral_id,
                "patient_id": patient_id,
                "clinician_id": rng.choice(service_clinicians[r["service"]]),
                "contact_date": iso(contact_date),
                "contact_type": rng.choice(CONTACT_TYPES),
                "duration_minutes": str(duration),
                "last_modified": iso(datetime.combine(contact_date or as_of, datetime.min.time())),
            })
            contact_no += 1

    # Duplicate contact rows: the same contact exported twice
    for c in rng.sample(contacts, k=max(1, int(len(contacts) * 0.02))):
        contacts.append(dict(c))

    rng.shuffle(contacts)
    return {
        "clinicians": clinicians,
        "patients": patients,
        "referrals": referrals,
        "contacts": contacts,
    }


def write_csv(rows: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_all(tables: dict[str, list[dict]], landing_dir: str) -> dict[str, str]:
    """Write each table to <landing_dir>/<table>/<table>.csv and return the paths."""
    paths = {}
    for name, rows in tables.items():
        path = os.path.join(landing_dir, name, f"{name}.csv")
        write_csv(rows, path)
        paths[name] = path
    return paths


def generate_patient_updates(base_patients_csv: str, fraction: float = 0.05,
                             seed: int = 7, as_of: date | None = None) -> list[dict]:
    """Simulate the next day's changes: some patients move GP practice or house.

    Returns updated copies of existing patient rows with a newer last_updated,
    which the gold layer turns into Type 2 history in dim_patient.
    """
    rng = random.Random(seed)
    as_of = as_of or date.today()
    with open(base_patients_csv, newline="", encoding="utf-8") as f:
        base = [row for row in csv.DictReader(f) if row["nhs_number"]]
    updates = []
    for row in rng.sample(base, k=max(1, int(len(base) * fraction))):
        changed = dict(row)
        if rng.random() < 0.6:
            changed["gp_practice"] = rng.choice([g for g in GP_PRACTICES if g != row["gp_practice"]])
        else:
            changed["postcode"] = postcode(rng)
        changed["last_updated"] = iso(datetime.combine(as_of, datetime.min.time()))
        updates.append(changed)
    return updates


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic NHS-style community health data")
    parser.add_argument("--output-dir", default="data/landing", help="where to write the CSV files")
    parser.add_argument("--patients", type=int, default=2000, help="number of patients")
    parser.add_argument("--seed", type=int, default=42, help="random seed, for repeatable data")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="reference date, YYYY-MM-DD")
    args = parser.parse_args()

    tables = generate(n_patients=args.patients, seed=args.seed, as_of=args.as_of)
    for name, path in write_all(tables, args.output_dir).items():
        print(f"{name:<11} {len(tables[name]):>7,} rows -> {path}")


if __name__ == "__main__":
    main()
