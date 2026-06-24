from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("OlympicConsumer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "olympic-events")
    .option("startingOffsets", "earliest")
    .load()
)

query = (
    df.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()