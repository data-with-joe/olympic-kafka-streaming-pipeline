from pyspark.sql.functions import col, count, when
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
     .appName("OlympicGold")
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
    .format('delta')
    .load("./delta/silver")
)
medal_df = silver_df.filter(col("Medal").isNotNull())

gold_df = (
    medal_df
    .groupBy("NOC")
    .agg(
        count(when(col("Medal") == "GOLD", True)).alias("Gold"),
        count(when(col("Medal") == "SILVER", True)).alias("Silver"),
        count(when(col("Medal") == "BRONZE", True)).alias("Bronze"),
        count("Medal").alias("Total_Medals")
    )
    .orderBy(col("Total_Medals").desc())
)

# Show results
gold_df.show(20, truncate=False)
query=(
    gold_df.write
    .format('delta')
    .mode('overwrite')
    .save("./delta/gold")
)
print('Gold layer created successfully')

#running: 
# spark-submit \
# --packages io.delta:delta-spark_2.13:4.3.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
# gold.py