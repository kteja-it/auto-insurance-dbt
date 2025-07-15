{{ config(materialized='table', schema='ANALYTICS') }}

SELECT
  policy_number,
  policy_bind_date,
  DATEDIFF(YEAR, policy_bind_date, '2025-07-09') AS policy_duration_years,
  COALESCE(policy_state, 'Unknown') AS policy_state,
  insured_zip,
  policy_csl,
  policy_deductable,
  policy_annual_premium,
  umbrella_limit
FROM {{ ref('stg_policies') }}