{{ config(materialized='view') }}

WITH cleaned_policies AS (
  SELECT
    policy_number,
    policy_bind_date,
    {{ coalesce_null('policy_state',"'unknown'")}},
    /*CASE 
      WHEN policy_state IN ('OH', 'IN', 'IL', 'PA', 'NY', 'SC', 'VA') THEN policy_state
      ELSE NULL
    END AS policy_state,*/
    policy_csl,
    policy_deductable,
    CASE 
      WHEN policy_annual_premium > 0 THEN policy_annual_premium
      ELSE NULL
    END AS policy_annual_premium,
    umbrella_limit,
    CASE 
      WHEN REGEXP_LIKE(insured_zip, '^[0-9]{5}$') THEN insured_zip
      ELSE NULL
    END AS insured_zip,
    DATEDIFF(day, policy_bind_date, CURRENT_DATE) AS policy_age_days
  FROM {{ source('raw', 'POLICIES') }}
)
SELECT
  policy_number,
  policy_bind_date,
  policy_state,
  policy_csl,
  policy_deductable,
  policy_annual_premium,
  umbrella_limit,
  insured_zip,
  policy_age_days
FROM cleaned_policies