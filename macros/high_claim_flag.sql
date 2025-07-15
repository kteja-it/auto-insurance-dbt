{% macro high_claim_flag(claim_amount) %}
  CASE
    WHEN {{ claim_amount }} > 50000 THEN 'High'
    WHEN {{ claim_amount }} BETWEEN 10000 AND 50000 THEN 'Medium'
    ELSE 'Low'
  END
{% endmacro %}