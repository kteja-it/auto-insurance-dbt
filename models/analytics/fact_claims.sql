{{ config(
    materialized='incremental',
    schema='ANALYTICS',
    unique_key='claim_id'
) }}

{% if is_incremental() %}
  {% set table_exists_query %}
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'RAW_ANALYTICS' 
      AND TABLE_NAME = 'FACT_CLAIMS' 
      AND COLUMN_NAME = 'DATE_ID'
  {% endset %}
  {% set table_exists_result = run_query(table_exists_query) %}
  {% set table_has_date_id = table_exists_result.columns[0][0] > 0 %}
{% else %}
  {% set table_has_date_id = false %}
{% endif %}

WITH max_date AS (
  {% if is_incremental() and table_has_date_id %}
    SELECT MAX(date_id) AS max_date_id
    FROM {{ this }}
  {% else %}
    SELECT TO_DATE('1900-01-01') AS max_date_id
  {% endif %}
)
SELECT
  c.claim_id,
  c.policy_number,
  p.policy_duration_years,
  p.policy_state,
  c.customer_id,
  cu.age_group,
  cu.insured_sex,
  cu.education_level,
  v.vehicle_id,
  v.auto_make,
  v.vehicle_age,
  c.incident_date AS date_id,
  c.incident_type,
  c.incident_severity_category,
  c.authorities_contacted,
  COALESCE(c.validated_total_claim_amount, 0) AS claim_amount,
  CASE WHEN c.fraud_reported = 'true' THEN 1 ELSE 0 END AS fraud_flag
FROM {{ ref('stg_claims') }} c
INNER JOIN {{ ref('dim_policies') }} p ON c.policy_number = p.policy_number
INNER JOIN {{ ref('dim_customers') }} cu ON c.customer_id = cu.customer_id
INNER JOIN {{ ref('dim_vehicles') }} v ON c.policy_number = v.policy_number
INNER JOIN {{ ref('dim_dates') }} d ON c.incident_date = d.date_id
{% if is_incremental() and table_has_date_id %}
  INNER JOIN max_date m ON c.incident_date >= m.max_date_id
{% endif %}