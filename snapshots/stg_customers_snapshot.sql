{% snapshot stg_customers_snapshot %}

{{ 
  config(
    target_schema='ANALYTICS',
    strategy='check',
    unique_key='customer_id',
    check_cols=['age_group', 'insured_sex']
  ) 
}}
-- Comment moved here: Monitoring changes in age_group and insured_sex
SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}