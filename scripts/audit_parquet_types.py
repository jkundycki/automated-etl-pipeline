# scripts/audit_parquet_types.py
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

FS = pafs.S3FileSystem()

BUCKET = "automated-etl-pipeline"
PREFIX = "weather/hourly/"  # trailing slash

expected = {
    "temperature_2m": pa.float64(),
    "relative_humidity_2m": pa.float64(),
    "apparent_temperature": pa.float64(),
    "precipitation": pa.float64(),
    "cloud_cover": pa.float64(),
    "windspeed_10m": pa.float64(),
    "winddirection_10m": pa.float64(),
}

def type_name(t: pa.DataType) -> str:
    s = str(t).upper()
    if s.startswith("TIMESTAMP"): return "TIMESTAMP"
    if s.startswith("DATE"): return "DATE"
    return s

# Recursively list parquet files under bucket/prefix (NO s3:// in selector)
selector = pa.fs.FileSelector(f"{BUCKET}/{PREFIX}", recursive=True)
infos = FS.get_file_info(selector)
files = [info.path for info in infos if info.is_file and info.path.endswith(".parquet")]
print(f"Found {len(files)} parquet files under s3://{BUCKET}/{PREFIX}")

bad = []

for path in files:
    # IMPORTANT: pass path WITHOUT s3:// when providing filesystem=FS
    try:
        tab = pq.read_table(path, filesystem=FS)
        schema = tab.schema
        mism = {}
        for col, want in expected.items():
            if col in schema.names:
                got = schema.field(col).type
                if type_name(got) != type_name(want):
                    mism[col] = (type_name(got), type_name(want))
        if mism:
            bad.append((f"s3://{path}", mism))
    except Exception as e:
        bad.append((f"s3://{path}", {"ERROR": str(e)}))

if not bad:
    print("OK: All files match expected numeric types (DOUBLE).")
else:
    print("MISMATCHES FOUND:")
    for uri, mism in bad:
        print(uri)
        if isinstance(mism, dict) and "ERROR" in mism:
            print(f"  ERROR: {mism['ERROR']}")
        else:
            for col, val in mism.items():
                if isinstance(val, (tuple, list)) and len(val) == 2:
                    got, want = val
                    print(f"  - {col}: got {got}, expected {want}")
                else:
                    print(f"  - {col}: {val}")
