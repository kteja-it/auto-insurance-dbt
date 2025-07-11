{{ config(materialized='table', schema='ANALYTICS') }}

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
  capital_loss
FROM {{ ref('stg_customers') }}