{{ config(materialized='view') }}

WITH cleaned_vehicles AS (
  SELECT
    v.vehicle_id,
    v.policy_number,
    INITCAP(v.auto_make) AS auto_make,
    v.auto_model,
    CASE
      WHEN v.auto_year BETWEEN 1990 AND 2025 THEN v.auto_year
      ELSE NULL
    END AS auto_year,
    (2025 - CASE WHEN v.auto_year BETWEEN 1990 AND 2025 THEN v.auto_year ELSE NULL END) AS vehicle_age
  FROM {{ source('raw', 'VEHICLES') }} v
  INNER JOIN {{ ref('stg_policies') }} p ON v.policy_number = p.policy_number
)
SELECT
  vehicle_id,
  policy_number,
  auto_make,
  auto_model,
  auto_year,
  vehicle_age
FROM cleaned_vehicles