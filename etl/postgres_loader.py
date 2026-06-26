from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("SilverToPostgres")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_df = (
    spark.read
    .format("delta")
    .load("./delta/silver")
)

(
    silver_df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/olympics")
    .option("dbtable", "olympic_events")
    .option("user", "mac")
    .option("password", "Password123!")
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

print("Silver data successfully loaded into PostgreSQL.")

# spark-submit \
# --packages io.delta:delta-spark_2.13:4.3.0,org.postgresql:postgresql:42.7.3 \
# postgres_loader.py