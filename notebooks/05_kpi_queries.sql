-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · KPI queries
-- MAGIC
-- MAGIC Example reporting queries on the gold star schema: the same KPIs a service
-- MAGIC lead would see in Power BI. Change the catalog and schema below if you used
-- MAGIC different names.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA nhs_lakehouse;

-- COMMAND ----------

-- Contacts per service per month
SELECT d.year, d.month, d.month_name, s.service_name, COUNT(*) AS contacts
FROM fact_contact f
JOIN dim_date d    ON f.date_key = d.date_key
JOIN dim_service s ON f.service_key = s.service_key
GROUP BY d.year, d.month, d.month_name, s.service_name
ORDER BY d.year, d.month, s.service_name;

-- COMMAND ----------

-- Active caseload: patients seen in the 90 days up to the latest contact, by service
WITH latest AS (SELECT MAX(d.date) AS max_date FROM fact_contact f JOIN dim_date d ON f.date_key = d.date_key)
SELECT s.service_name, COUNT(DISTINCT p.nhs_number) AS active_caseload_90_days
FROM fact_contact f
JOIN dim_date d    ON f.date_key = d.date_key
JOIN dim_service s ON f.service_key = s.service_key
JOIN dim_patient p ON f.patient_key = p.patient_key
CROSS JOIN latest
WHERE d.date > date_sub(latest.max_date, 90)
GROUP BY s.service_name
ORDER BY active_caseload_90_days DESC;

-- COMMAND ----------

-- Contact mix and average duration by service and contact type
SELECT s.service_name, f.contact_type,
       COUNT(*) AS contacts,
       ROUND(AVG(f.duration_minutes), 1) AS avg_minutes
FROM fact_contact f
JOIN dim_service s ON f.service_key = s.service_key
GROUP BY s.service_name, f.contact_type
ORDER BY s.service_name, contacts DESC;

-- COMMAND ----------

-- Data quality: issues found in the latest silver run
SELECT source_table, issue, COUNT(*) AS records
FROM (
    SELECT e.source_table, explode(split(e.issues, ';')) AS issue
    FROM exceptions e
    WHERE e.run_id = (SELECT run_id FROM audit_log WHERE step = 'silver' ORDER BY logged_at DESC LIMIT 1)
)
GROUP BY source_table, issue
ORDER BY records DESC;

-- COMMAND ----------

-- Pipeline health: the audit log for recent runs
SELECT run_id, step, entity, rows_read, rows_loaded, rows_rejected, status, message, logged_at
FROM audit_log
ORDER BY logged_at DESC
LIMIT 50;
