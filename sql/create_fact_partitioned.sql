CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_revenue_partitioned (
  fact_id TEXT,
  user_id INTEGER NOT NULL,
  month DATE NOT NULL,
  plan_name VARCHAR(50),
  total_revenue NUMERIC(10,2)
)
PARTITION BY RANGE (month);

CREATE TABLE IF NOT EXISTS gold.fact_revenue_2025_01
PARTITION OF gold.fact_revenue_partitioned
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS gold.fact_revenue_2025_02
PARTITION OF gold.fact_revenue_partitioned
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE INDEX IF NOT EXISTS idx_fact_user
ON gold.fact_revenue_partitioned (user_id);

CREATE INDEX IF NOT EXISTS idx_fact_plan
ON gold.fact_revenue_partitioned (plan_name);

INSERT INTO gold.fact_revenue_partitioned
SELECT fact_id, user_id, month, plan_name, total_revenue
FROM gold.fact_revenue
WHERE month >= '2025-01-01'
  AND month < '2025-03-01';
