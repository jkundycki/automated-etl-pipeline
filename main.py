# main.py
import os
from datetime import date
from etl.extract import extract_all
from etl.transform import transform_hourly, transform_daily, dq_assertions
from etl.load import load_hourly, load_daily

def main(run_day: date | None = None):
    print("Extracting...")
    # extract_all should now return ONE combined DataFrame with both cities and a 'location' column
    raw = extract_all(day=run_day)

    print("Transforming...")
    hourly = transform_hourly(raw)      # keep 'location', 'timestamp', 'date', 'year','month','day'
    daily  = transform_daily(hourly)    # build daily FROM hourly so both cities flow through

    # quick visibility while you’re validating
    try:
        print("Locations in hourly:", sorted(hourly["location"].unique().tolist()))
        print("Locations in daily :", sorted(daily["location"].unique().tolist()))
    except Exception:
        pass

    # basic DQ checks you already have
    dq_assertions(hourly)
    dq_assertions(daily)

    print("Loading to S3...")
    h_rows = load_hourly(hourly)
    d_rows = load_daily(daily)
    print(f"Done. Wrote rows -> hourly: {h_rows}, daily: {d_rows}")

if __name__ == "__main__":
    # Optional backfill via env var (YYYY-MM-DD). If unset, runs “today”.
    day_str = os.getenv("BACKFILL_DATE")
    run_day = date.fromisoformat(day_str) if day_str else date.today()
    main(run_day)
