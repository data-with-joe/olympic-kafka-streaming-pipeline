from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp

# Olympic dataset schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Height", IntegerType(), True),
    StructField("Weight", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("NOC", StringType(), True),
    StructField("Games", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Season", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Sport", StringType(), True),
    StructField("Event", StringType(), True),
    StructField("Medal", StringType(), True)
])

# Create Spark Session
builder = (
    SparkSession.builder
    .appName("OlympicBronzeConsumer")
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

# Read stream from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "olympic-events")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka value from bytes -> string -> JSON
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(
        from_json(
            col("json"),
            schema
        ).alias("data")
    )
    .select("data.*")
)
bronze_df = parsed_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# Display records
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "./checkpoints/bronze"
    )
    .start("./delta/bronze")
)

query.awaitTermination()