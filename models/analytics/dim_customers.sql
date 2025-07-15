{{ config(
    materialized='table',
    schema='ANALYTICS'
) }}

WITH latest_snapshot AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dbt_valid_from DESC) AS rn
  FROM {{ ref('stg_customers_snapshot') }}
  WHERE dbt_valid_to IS NULL OR dbt_valid_to > CURRENT_DATE
)
SELECT
  customer_id,
  age,
  age_group,
  insured_sex,
  education_level,
  insured_occupation,
  insured_hobbies,
  insured_relationship,
  capital_gains,
  capital_loss,
  dbt_valid_from AS effective_date,
  COALESCE(dbt_valid_to, '9999-12-31') AS end_date
FROM latest_snapshot
WHERE rn = 1