{{ config(materialized='table') }}

select 
POLICY_NUMBER,
POLICY_BIND_DATE,
POLICY_STATE,
POLICY_CSL,
POLICY_DEDUCTABLE,
POLICY_ANNUAL_PREMIUM,
UMBRELLA_LIMIT,
INSURED_ZIP
from {{ ref('stg_policies') }}