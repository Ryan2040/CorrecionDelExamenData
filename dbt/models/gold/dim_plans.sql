{{ config(materialized='table', schema='gold') }}

SELECT
  plan_name,
  messages_included,
  mb_per_month_included,
  ROUND(mb_per_month_included / 1024.0, 2) AS gb_per_month_included,
  minutes_included,
  usd_monthly_pay,
  usd_per_gb,
  usd_per_message,
  usd_per_minute
FROM {{ source('stg_plans', 'megaline_plans') }}
