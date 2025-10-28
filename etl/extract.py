import os
import json
import requests
from datetime import date
import pandas as pd

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def _locations_from_env():
    raw = os.getenv("WEATHER_LOCATIONS", '[{"name":"seattle","lat":47.6062,"lon":-122.3321}]')
    return json.loads(raw)

def fetch_weather_for_location(loc, target_date=None):
    """Return two DataFrames: hourly_df, daily_df for a single location."""
    target_date = target_date or date.today().isoformat()
    params = {
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "timezone": "auto",
        "start_date": target_date,
        "end_date": target_date,
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "snowfall",
            "cloud_cover",
            "windspeed_10m",
            "winddirection_10m"
        ]),
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
        ]),
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()

    # Hourly
    h = js.get("hourly", {})
    hourly_df = pd.DataFrame(h)
    if not hourly_df.empty:
        hourly_df["time"] = pd.to_datetime(hourly_df["time"])
        hourly_df["location"] = loc["name"]

    # Daily
    d = js.get("daily", {})
    daily_df = pd.DataFrame(d)
    if not daily_df.empty:
        daily_df["time"] = pd.to_datetime(daily_df["time"])
        daily_df["location"] = loc["name"]

    return hourly_df, daily_df

def extract_all(target_date=None):
    """Fetch all locations; return concatenated hourly_df, daily_df."""
    hourly_all, daily_all = [], []
    for loc in _locations_from_env():
        h, d = fetch_weather_for_location(loc, target_date)
        if not h.empty:
            hourly_all.append(h)
        if not d.empty:
            daily_all.append(d)
    hourly = pd.concat(hourly_all, ignore_index=True) if hourly_all else pd.DataFrame()
    daily  = pd.concat(daily_all,  ignore_index=True) if daily_all  else pd.DataFrame()
    return hourly, daily
