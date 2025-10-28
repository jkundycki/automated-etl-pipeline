import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd

BUCKET = os.getenv("WEATHER_BUCKET", "automated-etl-pipeline")

def _write_partitioned_parquet(df: pd.DataFrame, base_prefix: str):
    if df.empty:
        return 0
    fs = pafs.S3FileSystem()
    rows = 0
    for keys, part in df.groupby(["location", "year", "month", "day"]):
        loc, y, m, d = keys
        key = f"{base_prefix}/location={loc}/year={y}/month={m:02d}/day={d:02d}/part-0000.parquet"
        table = pa.Table.from_pandas(part, preserve_index=False)
        with fs.open_output_stream(f"{BUCKET}/{key}") as out:
            pq.write_table(table, out)
        rows += len(part)
    return rows

def load_hourly(df: pd.DataFrame) -> int:
    return _write_partitioned_parquet(df, "weather/hourly")

def load_daily(df: pd.DataFrame) -> int:
    return _write_partitioned_parquet(df, "weather/daily")
