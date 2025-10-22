from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, when
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("FraudStreamingConsumer") \
    .getOrCreate()

schema = StructType([
    StructField("Time", DoubleType()),
    StructField("Amount", DoubleType()),
    StructField("Class", IntegerType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud_topic") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

fraud_count = json_df.groupBy().agg(
    count(when(col("Class") == 1, True)).alias("fraudes_detectados")
)

query = fraud_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
