# spark_traffic_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "traffic-sensors"
ALERT_TOPIC = "Critical-Traffic"
PARQUET_OUTPUT = "./reports/traffic_windows"
CHECKPOINT = "./spark_checkpoints/traffic"

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True)
])

spark = SparkSession.builder     .appName("SmartCityTraffic")     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")     .getOrCreate()

raw = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)     .option("subscribe", SOURCE_TOPIC)     .option("startingOffsets", "latest")     .load()

events = raw.selectExpr("CAST(value AS STRING) as json_str")     .select(from_json(col("json_str"), schema).alias("data"))     .select(
        col("data.sensor_id").alias("sensor_id"),
        to_timestamp(col("data.timestamp")).alias("ts"),
        col("data.vehicle_count").alias("vehicle_count"),
        col("data.avg_speed").alias("avg_speed")
    )

agg = events.withWatermark("ts", "10 minutes")     .groupBy(window(col("ts"), "5 minutes"), col("sensor_id"))     .agg(
        expr("sum(vehicle_count) as total_vehicles"),
        expr("avg(avg_speed) as avg_speed_window")
    ).select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_vehicles"),
        col("avg_speed_window")
    )

parquet_query = agg.writeStream     .outputMode("append")     .format("parquet")     .option("path", PARQUET_OUTPUT)     .option("checkpointLocation", CHECKPOINT + "/parquet")     .start()

alerts = agg.filter(col("avg_speed_window") < 10.0)     .selectExpr("to_json(named_struct('sensor_id', sensor_id, 'window_start', window_start, 'window_end', window_end, 'total_vehicles', total_vehicles, 'avg_speed', avg_speed_window)) as value")

alerts_query = alerts.writeStream     .format("kafka")     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)     .option("topic", ALERT_TOPIC)     .option("checkpointLocation", CHECKPOINT + "/alerts")     .start()

spark.streams.awaitAnyTermination()
