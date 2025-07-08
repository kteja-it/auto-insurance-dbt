{{ config(materialized='table') }}

SELECT
  customer_id,
  age,
  insured_sex,
  insured_education_level,
  insured_occupation,
  insured_hobbies,
  insured_relationship,
  capital_gains,
  capital_loss
From {{ ref('stg_customers') }}