{{ config(materialized='view', schema='STAGING') }}

SELECT customer_id, age
FROM {{ ref('stg_customers') }}


