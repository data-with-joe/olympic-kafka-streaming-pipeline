from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName('read_gold')
    .config(
        "spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
df = spark.read.format('delta').load('./delta/gold')
df.show(20, truncate=False)

# reading: spark-submit \
# --packages io.delta:delta-spark_2.13:4.3.0 \
# read_gold.py