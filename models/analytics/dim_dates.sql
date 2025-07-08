{{ config(materialized='table') }}

WITH date_range AS (
  SELECT DATEADD(day, seq4(), '2015-01-01') AS incident_date
  FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- Covers Jan-Feb 2015
)
SELECT
  incident_date,
  YEAR(incident_date) AS year,
  MONTH(incident_date) AS month,
  DAY(incident_date) AS day,
  QUARTER(incident_date) AS quarter
FROM date_range