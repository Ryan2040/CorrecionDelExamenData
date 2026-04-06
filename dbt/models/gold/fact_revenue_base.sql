{{ config(materialized='table', schema='gold') }}

WITH monthly_calls AS (
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
)

SELECT
  s.user_id,
  s.month,
  u.plan AS plan_name,
  COALESCE(mc.total_calls, 0) AS total_calls,
  COALESCE(mc.total_minutes, 0) AS total_minutes,
  COALESCE(mm.total_messages, 0) AS total_messages,
  COALESCE(mi.total_mb_used, 0) AS total_mb_used
FROM spine s
JOIN {{ source('stg_users', 'megaline_users') }} u
  ON s.user_id = u.user_id
JOIN {{ source('stg_plans', 'megaline_plans') }} p
  ON u.plan = p.plan_name
LEFT JOIN monthly_calls mc
  ON s.user_id = mc.user_id
 AND s.month = mc.month
LEFT JOIN monthly_messages mm
  ON s.user_id = mm.user_id
 AND s.month = mm.month
LEFT JOIN monthly_internet mi
  ON s.user_id = mi.user_id
 AND s.month = mi.month
