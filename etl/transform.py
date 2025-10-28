import pandas as pd

def _add_partitions(df: pd.DataFrame, time_col: str = "time") -> pd.DataFrame:
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df[time_col]).dt.date
    dt = pd.to_datetime(df[time_col])
    df["year"]  = dt.dt.year.astype("int16")
    df["month"] = dt.dt.month.astype("int8")
    df["day"]   = dt.dt.day.astype("int8")
    return df

def transform_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    # basic sanity / type coercion
    for col in df.columns:
        if col not in ("time", "location"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return _add_partitions(df, "time")

def transform_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if col not in ("time", "location"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return _add_partitions(df, "time")

def dq_assertions(df: pd.DataFrame):
    if df.empty:
        return
    assert df["time"].notna().all(), "Null timestamps"
    assert df["location"].notna().all(), "Missing location"
