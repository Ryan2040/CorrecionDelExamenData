{{ config(materialized='table', schema='gold') }}

SELECT
  user_id,
  first_name,
  last_name,
  first_name || ' ' || last_name AS full_name,
  age,
  city,
  reg_date,
  churn_date,
  plan AS plan_name,
  CASE
    WHEN churn_date IS NULL THEN TRUE
    ELSE FALSE
  END AS is_active
FROM {{ source('stg_users', 'megaline_users') }}
