{{ config(materialized='table', schema='gold') }}

WITH date_spine AS (
  SELECT generate_series(
    '2015-01-01'::DATE,
    CURRENT_DATE,
    '1 day'::INTERVAL
  )::DATE AS full_date
)

SELECT
  TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_id,
  full_date,
  EXTRACT(YEAR FROM full_date)::INTEGER AS year,
  EXTRACT(MONTH FROM full_date)::INTEGER AS month,
  TO_CHAR(full_date, 'YYYY-MM') AS year_month,
  TO_CHAR(full_date, 'Q')::INTEGER AS quarter,
  TRIM(TO_CHAR(full_date, 'Month')) AS month_name
FROM date_spine
