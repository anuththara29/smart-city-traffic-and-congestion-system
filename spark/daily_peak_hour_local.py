# daily_peak_hour_local.py
# A lightweight alternative using pyarrow/pandas if you don't want Spark for batch reporting.
import os
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime

PARQUET_PATH = "./reports/traffic_windows"

def main(date_str=None):
    if not os.path.exists(PARQUET_PATH):
        print("Parquet path not found:", PARQUET_PATH)
        return
    # read all parquet files
    table = pq.read_table(PARQUET_PATH)
    df = table.to_pandas()
    df['window_start'] = pd.to_datetime(df['window_start'])
    df['hour'] = df['window_start'].dt.hour
    agg = df.groupby(['sensor_id','hour'])['total_vehicles'].sum().reset_index()
    idx = agg.groupby(['sensor_id'])['total_vehicles'].idxmax()
    peak = agg.loc[idx]
    out_csv = f"./reports/peak_hour_{datetime.now().strftime('%Y-%m-%d')}.csv"
    peak.to_csv(out_csv, index=False)
    print("Wrote peak hour report to", out_csv)

if __name__ == "__main__":
    main()
