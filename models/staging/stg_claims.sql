{{ config(materialized='view') }}

SELECT
  claim_id,
  policy_number,
  incident_date,
  incident_type,
  NULLIF(collision_type, '?') AS collision_type,
  incident_severity,
  NULLIF(authorities_contacted, '?') AS authorities_contacted,
  incident_state,
  incident_city,
  incident_location,
  incident_hour_of_the_day,
  number_of_vehicles_involved,
  NULLIF(property_damage, '?') AS property_damage,
  bodily_injuries,
  witnesses,
  NULLIF(police_report_available, '?') AS police_report_available,
  total_claim_amount,
  injury_claim,
  property_claim,
  vehicle_claim,
  fraud_reported
FROM {{ source('raw', 'CLAIMS') }}