{{ config(materialized='table', schema='gold') }}

WITH
users AS (
  SELECT * FROM {{ source('stg_users', 'megaline_users') }}
),

plans AS (
  SELECT * FROM {{ source('stg_plans', 'megaline_plans') }}
),

monthly_calls AS (
  SELECT
    user_id,
    DATE_TRUNC('month', call_date)::DATE AS month,
    COUNT(*) AS total_calls,
    CEIL(SUM(GREATEST(duration, 0)))::INTEGER AS total_minutes
  FROM {{ source('stg_calls', 'megaline_calls') }}
  GROUP BY 1, 2
),

monthly_messages AS (
  SELECT
    user_id,
    DATE_TRUNC('month', message_date)::DATE AS month,
    COUNT(*) AS total_messages
  FROM {{ source('stg_messages', 'megaline_messages') }}
  GROUP BY 1, 2
),

monthly_internet AS (
  SELECT
    user_id,
    DATE_TRUNC('month', session_date)::DATE AS month,
    SUM(GREATEST(mb_used, 0)) AS total_mb_used
  FROM {{ source('stg_internet', 'megaline_internet') }}
  GROUP BY 1, 2
),

spine AS (
  SELECT DISTINCT user_id, month FROM monthly_calls
  UNION
  SELECT DISTINCT user_id, month FROM monthly_messages
  UNION
  SELECT DISTINCT user_id, month FROM monthly_internet
),

joined AS (
  SELECT
    s.user_id,
    s.month,
    u.plan,
    COALESCE(mc.total_calls, 0) AS total_calls,
    COALESCE(mc.total_minutes, 0) AS total_minutes,
    COALESCE(mm.total_messages, 0) AS total_messages,
    COALESCE(mi.total_mb_used, 0) AS total_mb_used,
    p.usd_monthly_pay,
    p.minutes_included,
    p.messages_included,
    p.mb_per_month_included,
    p.usd_per_minute,
    p.usd_per_message,
    p.usd_per_gb
  FROM spine s
  JOIN users u
    ON s.user_id = u.user_id
  JOIN plans p
    ON u.plan = p.plan_name
  LEFT JOIN monthly_calls mc
    ON s.user_id = mc.user_id AND s.month = mc.month
  LEFT JOIN monthly_messages mm
    ON s.user_id = mm.user_id AND s.month = mm.month
  LEFT JOIN monthly_internet mi
    ON s.user_id = mi.user_id AND s.month = mi.month
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['user_id', 'month']) }} AS fact_id,
  user_id,
  month,
  plan AS plan_name,
  total_calls,
  total_minutes,
  total_messages,
  ROUND(total_mb_used::NUMERIC, 2) AS total_mb_used,
  usd_monthly_pay,
  GREATEST(total_minutes - minutes_included, 0) AS excess_minutes,
  GREATEST(total_messages - messages_included, 0) AS excess_messages,
  GREATEST((total_mb_used - mb_per_month_included) / 1024.0, 0) AS excess_gb,
  GREATEST(total_minutes - minutes_included, 0) * usd_per_minute AS revenue_excess_minutes,
  GREATEST(total_messages - messages_included, 0) * usd_per_message AS revenue_excess_messages,
  GREATEST((total_mb_used - mb_per_month_included) / 1024.0, 0) * usd_per_gb AS revenue_excess_gb,
  ROUND((
    usd_monthly_pay
    + GREATEST(total_minutes - minutes_included, 0) * usd_per_minute
    + GREATEST(total_messages - messages_included, 0) * usd_per_message
    + GREATEST((total_mb_used - mb_per_month_included) / 1024.0, 0) * usd_per_gb
  )::NUMERIC, 2) AS total_revenue
FROM joined
