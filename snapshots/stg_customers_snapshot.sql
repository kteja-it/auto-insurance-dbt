{% snapshot stg_customers_snapshot %}

{{ 
  config(
    target_schema='ANALYTICS',
    strategy='check',
    check_cols=['age_group', 'insured_sex']
  ) 
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}