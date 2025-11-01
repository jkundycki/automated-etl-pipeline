import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd

BUCKET = os.getenv("WEATHER_BUCKET", "automated-etl-pipeline")

def _write_partitioned_parquet(df: pd.DataFrame, base_prefix: str):
    if df.empty:
        return 0

    # ✅ Enforce consistent numeric schema before writing to Parquet
    NUMERIC_SCHEMA = {
        "temperature_2m": "float64",
        "relative_humidity_2m": "float64",
        "apparent_temperature": "float64",
        "precipitation": "float64",
        "cloud_cover": "float64",
        "windspeed_10m": "float64",
        "winddirection_10m": "float64",
    }

    for col, dtype in NUMERIC_SCHEMA.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    fs = pafs.S3FileSystem()
    rows = 0

    # ✅ Group by partition columns (location/year/month/day)
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

