{{ config(materialized='view') }}

WITH cleaned_claims AS (
  SELECT
    c.claim_id,
    c.policy_number,
    cu.customer_id, -- From RAW.CUSTOMERS
    c.incident_date,
    c.incident_type,
    c.incident_severity,
    CASE
      WHEN c.incident_severity IN ('Major Damage', 'Total Loss') THEN 'Severe'
      WHEN c.incident_severity IN ('Minor Damage', 'Trivial Damage') THEN 'Non-Severe'
      ELSE 'Unknown'
    END AS incident_severity_category,
    c.authorities_contacted,
    CASE WHEN CAST(c.injury_claim AS STRING) = '?' THEN NULL ELSE CAST(c.injury_claim AS INTEGER) END AS injury_claim,
    CASE WHEN CAST(c.property_claim AS STRING) = '?' THEN NULL ELSE CAST(c.property_claim AS INTEGER) END AS property_claim,
    CASE WHEN CAST(c.vehicle_claim AS STRING) = '?' THEN NULL ELSE CAST(c.vehicle_claim AS INTEGER) END AS vehicle_claim,
    c.total_claim_amount,
    CASE
      WHEN c.total_claim_amount = COALESCE(CAST(c.injury_claim AS INTEGER), 0) + COALESCE(CAST(c.property_claim AS INTEGER), 0) + COALESCE(CAST(c.vehicle_claim AS INTEGER), 0)
      THEN c.total_claim_amount
      ELSE NULL
    END AS validated_total_claim_amount,
    c.fraud_reported,
    ROW_NUMBER() OVER (PARTITION BY c.claim_id ORDER BY cu.customer_id) AS rn
  FROM {{ source('raw', 'CLAIMS') }} c
  INNER JOIN {{ source('raw', 'POLICIES') }} p ON c.policy_number = p.policy_number
  INNER JOIN {{ source('raw', 'CUSTOMERS') }} cu ON p.insured_zip = SPLIT_PART(cu.customer_id, '_', 1)
)
SELECT
  claim_id,
  policy_number,
  customer_id,
  incident_date,
  incident_type,
  incident_severity,
  incident_severity_category,
  authorities_contacted,
  injury_claim,
  property_claim,
  vehicle_claim,
  validated_total_claim_amount,
  fraud_reported
FROM cleaned_claims
WHERE rn = 1