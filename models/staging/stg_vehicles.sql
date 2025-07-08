{{ config(materialized='view') }}

SELECT
  vehicle_id,
  policy_number,
  auto_make,
  auto_model,
  auto_year
FROM {{ source('raw', 'VEHICLES') }}