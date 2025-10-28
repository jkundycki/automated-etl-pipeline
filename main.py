from etl.extract import extract_all
from etl.transform import transform_hourly, transform_daily, dq_assertions
from etl.load import load_hourly, load_daily

def main():
    print("Extracting...")
    hourly_raw, daily_raw = extract_all()

    print("Transforming...")
    hourly = transform_hourly(hourly_raw)
    daily  = transform_daily(daily_raw)
    dq_assertions(hourly)
    dq_assertions(daily)

    print("Loading to S3...")
    h_rows = load_hourly(hourly)
    d_rows = load_daily(daily)
    print(f"Done. Wrote rows -> hourly: {h_rows}, daily: {d_rows}")

if __name__ == "__main__":
    main()
