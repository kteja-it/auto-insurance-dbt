{{ config(materialized='table') }}


WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="'2014-01-01'",
      end_date="'2025-12-31'"
   ) }}
)
SELECT
  date_day AS date_id,
  YEAR(date_day) AS year,
  MONTH(date_day) AS month,
  DAY(date_day) AS day,
  QUARTER(date_day) AS quarter,
  DAYOFWEEK(date_day) AS day_of_week,
  DAYNAME(date_day) AS day_name,
  MONTHNAME(date_day) AS month_name,
  CASE
    WHEN MONTH(date_day) IN (12, 1, 2) THEN 'Winter'
    WHEN MONTH(date_day) IN (3, 4, 5) THEN 'Spring'
    WHEN MONTH(date_day) IN (6, 7, 8) THEN 'Summer'
    ELSE 'Fall'
  END AS season
FROM date_spine