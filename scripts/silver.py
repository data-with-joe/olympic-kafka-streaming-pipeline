from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, current_timestamp, upper

builder = (
    SparkSession.builder
    .appName("OlympicSilver")
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

bronze_df = (
    spark.readStream
    .format("delta")
    .load("./delta/bronze")
)

silver_df = (
    bronze_df

    .filter(col("ID").isNotNull())

    
    .filter(col("Name").isNotNull())

    
    .dropDuplicates(["ID", "Games", "Event"])

    
    .withColumn(
        "Medal",
        upper(col("Medal"))
    )

    .withColumn(
        "silver_processed_time",
        current_timestamp()
    )
)


query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "./checkpoints/silver"
    )
    .start("./delta/silver")
)

query.awaitTermination()

#running: 
# spark-submit \
# --packages io.delta:delta-spark_2.13:4.3.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
# silver.py