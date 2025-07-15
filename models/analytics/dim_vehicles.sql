{{ config(materialized='table', schema='ANALYTICS') }}

SELECT
  vehicle_id,
  policy_number,
  auto_make,
  auto_model,
  auto_year,
  vehicle_age
FROM {{ ref('stg_vehicles') }}