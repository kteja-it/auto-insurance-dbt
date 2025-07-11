{{ config(materialized='table', schema='ANALYTICS') }}
{{ config(
    materialized = 'incremental',
    schema='ANALYTICS',
    unique_key = 'claim_id'
) }}


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
{% if is_incremental() %}
    where c.incident_date >= (select max(date_id) from {{this}} )
{% endif %}