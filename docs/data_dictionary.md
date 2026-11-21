# Data dictionary

All data is synthetic. NHS numbers are in the 999 test range.

## Source extracts (landing and bronze)

Bronze tables hold these columns as text, plus `_source_file`, `_ingested_at` and `_run_id`.

### patients

| Column | Description |
| --- | --- |
| patient_id | Internal patient ID from the clinical system. A person registered twice has two IDs. |
| nhs_number | 10-digit NHS number with a Modulus 11 check digit. May be missing or invalid. |
| first_name, last_name | Synthetic names |
| date_of_birth | yyyy-MM-dd |
| postcode | East London-style postcode |
| gp_practice | Synthetic GP practice code |
| last_updated | When the record was last changed; the latest record wins during de-duplication |

### referrals

| Column | Description |
| --- | --- |
| referral_id | Unique referral ID |
| patient_id | Patient the referral is for |
| service | Wound Care, Lymphoedema, Podiatry or Tissue Viability |
| referral_source | GP, District Nurse, Hospital or Self-referral |
| referral_date | yyyy-MM-dd |

### contacts

| Column | Description |
| --- | --- |
| contact_id | Unique contact ID. Repeated exports create duplicates. |
| referral_id | Referral the contact belongs to |
| patient_id | Patient seen |
| clinician_id | Clinician who carried out the contact |
| contact_date | yyyy-MM-dd |
| contact_type | Face to face, Telephone or Video |
| duration_minutes | Length of the contact in minutes |
| last_modified | When the record was last changed |

### clinicians

| Column | Description |
| --- | --- |
| clinician_id | Unique clinician ID |
| clinician_name | Synthetic name |
| role | Specialist Nurse, Clinical Lead, Podiatrist or Healthcare Assistant |
| service | The clinician's home service |

## Silver

| Table | Description |
| --- | --- |
| silver_patients | One row per NHS number (the latest record), typed and standardised |
| silver_patient_xref | patient_id → master_patient_id and nhs_number, including old duplicate IDs |
| silver_referrals | Valid referrals, with master_patient_id and nhs_number |
| silver_contacts | Valid contacts, with referral date, service, master_patient_id and nhs_number |
| exceptions | run_id, source_table, record_id, issues (';'-separated rule names), record_json, detected_at |

## Gold

| Table | Grain | Key columns |
| --- | --- | --- |
| fact_contact | One row per clinical contact | contact_key, patient_key, date_key, service_key, clinician_key, contact_type, duration_minutes |
| dim_patient | One row per patient version (Type 2) | patient_key, nhs_number, gp_practice, postcode, valid_from, valid_to, is_current |
| dim_date | One row per calendar day | date_key (yyyyMMdd), date, month_name, quarter, financial_year (April to March), is_weekend |
| dim_service | One row per service | service_key, service_name |
| dim_clinician | One row per clinician | clinician_key, clinician_id, clinician_name, role, home_service |
| vw_contact_reporting | Flat view for Power BI | Fact joined to every dimension |

## Audit

| Table | Description |
| --- | --- |
| audit_log | run_id, step (bronze, silver, gold), entity, rows_read, rows_loaded, rows_rejected, status, message, logged_at |
