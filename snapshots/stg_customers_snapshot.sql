{% snapshot stg_customers_snapshot %}

{{ 
  config(
    target_schema='ANALYTICS',
    strategy='check',
    unique_key='customer_id',
    check_cols=['age_group', 'insured_sex', 'age', 'insured_education_level']  -- Updated to include new columns
  ) 
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}