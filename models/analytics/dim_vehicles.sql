{{ config(materialized='table') }}

SELECT
  vehicle_id,
  policy_number,
  auto_make,
  auto_model,
  auto_year
FROM {{ ref('stg_vehicles') }}