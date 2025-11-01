# etl/transform.py
import pandas as pd
import numpy as np

TIME_CANDIDATES = ["timestamp", "time"]  # accept either

def _pick_time_col(df: pd.DataFrame) -> str:
    for c in TIME_CANDIDATES:
        if c in df.columns:
            return c
    raise KeyError(f"None of the expected time columns found: {TIME_CANDIDATES}. Columns={list(df.columns)}")

def _add_partitions(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    # normalize to UTC datetime
    ts = pd.to_datetime(df[time_col], utc=True)
    df = df.copy()
    df["date"]  = ts.dt.date
    df["year"]  = ts.dt.year
    df["month"] = ts.dt.month
    df["day"]   = ts.dt.day
    return df

def _to_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df

def transform_hourly(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    time_col = _pick_time_col(raw)
    df = _add_partitions(raw, time_col)

    # ensure location survives and is a plain string
    if "location" not in df.columns:
        raise KeyError("Expected 'location' column to be present after extract_all().")

    df["location"] = df["location"].astype("string")

    # cast numerics consistently
    numeric_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "cloud_cover",
        "windspeed_10m",
        "winddirection_10m",
    ]
    df = _to_float(df, numeric_cols)

    # keep a tidy hourly schema (include both time column and derived partitions)
    keep = [
        time_col, "location",
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation", "cloud_cover", "windspeed_10m", "winddirection_10m",
        "date", "year", "month", "day",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep]

def transform_daily(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return hourly

    # we’ll aggregate from hourly → daily, preserving location
    req = {"location", "date"}
    missing = req - set(hourly.columns)
    if missing:
        raise KeyError(f"transform_daily: missing required columns: {missing}")

    aggs = {
        "temperature_2m": "mean",
        "relative_humidity_2m": "mean",
        "apparent_temperature": "mean",
        "precipitation": "sum",
        "cloud_cover": "mean",
        "windspeed_10m": "mean",
        "winddirection_10m": "mean",
    }
    grp = hourly.groupby(["location", "date"], as_index=False).agg(aggs)

    # re-add partitions for daily (based on date)
    ts = pd.to_datetime(grp["date"])
    grp["year"]  = ts.dt.year
    grp["month"] = ts.dt.month
    grp["day"]   = ts.dt.day

    # clean column names (optional; Athena is fine with these)
    return grp

def dq_assertions(df: pd.DataFrame) -> None:
    """Simple DQ checks: required cols present, no NaNs in keys, numerics are finite."""
    if df.empty:
        return
    # required keys
    for c in ["location", "date"]:
        if c not in df.columns:
            raise KeyError(f"dq_assertions: missing required column '{c}'")
        if df[c].isna().any():
            raise ValueError(f"dq_assertions: nulls found in key column '{c}'")

    # numeric columns to validate if present
    numeric_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "cloud_cover",
        "windspeed_10m",
        "winddirection_10m",
    ]
    for c in numeric_cols:
        if c in df.columns:
            vals = pd.to_numeric(df[c], errors="coerce")
            if vals.isna().any():
                raise ValueError(f"dq_assertions: non-numeric/NaN values in '{c}'")
            if not np.isfinite(vals.to_numpy()).all():
                raise ValueError(f"dq_assertions: non-finite values in '{c}'")
