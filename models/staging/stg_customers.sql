{{ config(materialized='view') }}

WITH cleaned_customers AS (
  SELECT
    customer_id,
    age,
    CASE
      WHEN age < 25 THEN 'Young Adult'
      WHEN age BETWEEN 25 AND 40 THEN 'Adult'
      WHEN age BETWEEN 41 AND 60 THEN 'Middle-Aged'
      WHEN age > 60 THEN 'Senior'
      ELSE 'Unknown'
    END AS age_group,
    insured_sex,
    CASE
      WHEN insured_education_level = 'MD' THEN 'Medical Degree'
      WHEN insured_education_level = 'PhD' THEN 'Doctorate'
      WHEN insured_education_level = 'Masters' THEN 'Masters Degree'
      WHEN insured_education_level = 'Bachelors' THEN 'Bachelors Degree'
      WHEN insured_education_level = 'Associate' THEN 'Associate Degree'
      WHEN insured_education_level = 'College' THEN 'Some College'
      WHEN insured_education_level = 'JD' THEN 'Juris Doctor'
      ELSE 'Unknown'
    END AS education_level,
    insured_occupation,
    COALESCE(NULLIF(insured_hobbies, ''), 'Unknown') AS insured_hobbies,
    insured_relationship,
    CASE
      WHEN capital_gains >= 0 THEN capital_gains
      ELSE 0
    END AS capital_gains,
    CASE
      WHEN capital_loss <= 0 THEN capital_loss
      ELSE 0
    END AS capital_loss
  FROM {{ source('raw', 'CUSTOMERS') }}
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
  capital_loss
FROM cleaned_customers