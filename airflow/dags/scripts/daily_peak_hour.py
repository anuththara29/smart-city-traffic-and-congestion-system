# daily_peak_hour.py
# This script reads parquet files written by the streaming job and computes peak hour per junction.
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col

def main(date_str):
    spark = SparkSession.builder.appName("DailyPeakHour").getOrCreate()
    parquet_path = "./reports/traffic_windows"  # adjust if needed
    if not os.path.exists(parquet_path):
        print("Parquet path not found:", parquet_path)
        return
    df = spark.read.parquet(parquet_path)
    # df schema: sensor_id, window_start, window_end, total_vehicles, avg_speed_window
    # We'll extract hour from window_start
    df2 = df.withColumn("hour", hour(col("window_start")))
    # Aggregate vehicles by sensor and hour
    agg = df2.groupBy("sensor_id", "hour").agg({"total_vehicles":"sum"}).withColumnRenamed("sum(total_vehicles)","vehicles_sum")
    # For each sensor, find hour with max vehicles
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    w = Window.partitionBy("sensor_id").orderBy(desc("vehicles_sum"))
    ranked = agg.withColumn("rn", row_number().over(w)).filter(col("rn")==1).select("sensor_id","hour","vehicles_sum")
    out_csv = f"./reports/peak_hour_{date_str}.csv"
    ranked.coalesce(1).write.csv(out_csv, header=True, mode="overwrite")
    print("Wrote peak hour report to", out_csv)
    spark.stop()

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv)>1 else datetime.now().strftime("%Y-%m-%d")
    main(date_str)
