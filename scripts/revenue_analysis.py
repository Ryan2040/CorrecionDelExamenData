import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DB_HOST = os.getenv("POSTGRES_HOST", "warehouse")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "source")
DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")

spark = (
    SparkSession.builder
    .appName("Revenue_Analysis_2025")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

fact_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "gold.fact_revenue")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

fact_2025 = fact_df.filter(F.year("month") == 2025)

result = (
    fact_2025.groupBy("plan_name")
    .agg(F.round(F.avg("total_revenue"), 2).alias("avg_monthly_revenue"))
    .orderBy(F.desc("avg_monthly_revenue"))
)

result.show(truncate=False)
