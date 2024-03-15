import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("PurchasingEventAggregator") \
    .getOrCreate()

kafka_host = "localhost"  # Update with your Kafka host if different
kafka_topic = "purchasing_events"  # Update with your Kafka topic name
output_path = "file:///C:/Users/Satriyo Wisnu/dibimbing_spark_airflow/spark-scripts/output" # Update with your desired output directory

# Read from Kafka topic
stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for parsing JSON from Kafka
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("furniture", StringType()) \
    .add("color", StringType()) \
    .add("price", IntegerType()) \
    .add("ts", TimestampType())  # Change the type to TimestampType

# Convert value from Kafka to JSON string and then parse JSON with defined schema
parsed_df = stream_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json("value", schema).alias("data")).select("data.*")

# Aggregate total purchases over 10-minute windows
agg_df = parsed_df \
    .withWatermark("ts", "10 minutes") \
    .groupBy(F.window("ts", "10 minutes"), "ts") \
    .agg(F.sum("price").alias("running_total"))

# Write aggregated data to CSV
query = agg_df \
    .writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", output_path) \
    .option("checkpointLocation", "file:///C:/Users/Satriyo Wisnu/dibimbing_spark_airflow/spark-scripts/checkpoint") \
    .start()

query.awaitTermination()