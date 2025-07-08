{{ config(materialized='table') }}

SELECT
  c.claim_id,
  c.policy_number,
  c.incident_date,
  c.incident_type,
  c.collision_type,
  c.incident_severity,
  c.authorities_contacted,
  c.incident_state,
  c.incident_city,
  c.incident_location,
  c.incident_hour_of_the_day,
  c.number_of_vehicles_involved,
  c.property_damage,
  c.bodily_injuries,
  c.witnesses,
  c.police_report_available,
  c.total_claim_amount,
  c.injury_claim,
  c.property_claim,
  c.vehicle_claim,
  c.fraud_reported,
  p.insured_zip,
  p.policy_state,
  p.policy_csl,
  p.policy_deductable,
  p.policy_annual_premium,
  p.umbrella_limit,
  cu.customer_id,
  cu.age,
  cu.insured_sex,
  cu.insured_education_level,
  cu.insured_occupation,
  v.vehicle_id,
  v.auto_make,
  v.auto_model,
  v.auto_year
FROM {{ ref('stg_claims') }} c
LEFT JOIN {{ ref('stg_policies') }} p ON c.policy_number = p.policy_number
LEFT JOIN {{ ref('stg_customers') }} cu ON CONCAT(p.insured_zip, '_', cu.age) = cu.customer_id
LEFT JOIN {{ ref('stg_vehicles') }} v ON c.policy_number = v.policy_number