<<<<<<< HEAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, to_timestamp, current_timestamp
from schema import get_iot_schema

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_INPUT_TOPIC = "iot-events"
CHECKPOINT_LOCATION = "./checkpoints/iot-streaming"
OUTPUT_PATH = "./output/valid_records"

def start_streaming_app():
    spark = SparkSession.builder \
        .appName("AdvancedSparkStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_payload")

    schema = get_iot_schema()
    parsed_df = json_df.withColumn("data", from_json(col("json_payload"), schema)) \
        .select("data.*", "json_payload")


    valid_events = parsed_df.filter(
        col("device_id").isNotNull() & 
        col("event_time").isNotNull() & 
        (col("temperature") != -999) &
        (col("temperature").isNotNull())
    ).withColumn("timestamp", to_timestamp(col("event_time")))

    invalid_events = parsed_df.filter(
        col("device_id").isNull() | 
        col("event_time").isNull() | 
        (col("temperature") == -999) |
        (col("temperature").isNull())
    )

    # Windowed Aggregations with Watermarks
    # Define a 10-minute watermark and aggregate by device_id in 5-minute windows
    windowed_aggregations = valid_events \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("device_id")
        ).agg(
            avg("temperature").alias("avg_temp"),
            count("*").alias("event_count")
        )

    # Sinks
    
    # Sink 1: Console for valid aggregations (debugging)
    query_valid = windowed_aggregations.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Sink 2: Console for invalid events (to see what's being rejected)
    query_invalid = invalid_events.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Sink 3: File sink for valid records (as requested)
    query_file = valid_events.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    query_valid.awaitTermination()
    query_invalid.awaitTermination()
    query_file.awaitTermination()

if __name__ == "__main__":
    start_streaming_app()
=======
# Write your streaming app code here
>>>>>>> 98a843ef9356e74eaa4bf0fea8e8c9beafb55213
