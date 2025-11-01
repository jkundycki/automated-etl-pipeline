# etl/extract.py
import os, json, requests, pandas as pd
from datetime import date

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"

# Parse locations from ENV (GitHub Actions already sets WEATHER_LOCATIONS)
def _load_locations():
    raw = os.getenv("WEATHER_LOCATIONS",
        '[{"name":"seattle","lat":47.6062,"lon":-122.3321},{"name":"newyork","lat":40.7128,"lon":-74.0060}]'
    )
    locs = json.loads(raw)
    # normalize
    out = []
    for l in locs:
        out.append({
            "name": str(l["name"]).strip().lower(),
            "lat": float(l["lat"]),
            "lon": float(l["lon"]),
        })
    return out

def _fetch_open_meteo(lat: float, lon: float, day: date | None = None) -> pd.DataFrame:
    hourly = ",".join([
        "temperature_2m","relative_humidity_2m","apparent_temperature",
        "precipitation","cloud_cover","windspeed_10m","winddirection_10m"
    ])
    params = {"latitude": lat, "longitude": lon, "hourly": hourly, "timezone": "UTC"}
    if day:
        ds = day.strftime("%Y-%m-%d")
        params["start_date"] = ds
        params["end_date"]   = ds

    r = requests.get(OPEN_METEO, params=params, timeout=30)
    r.raise_for_status()
    h = r.json()["hourly"]
    df = pd.DataFrame(h)
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["date"]  = df["timestamp"].dt.date
    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"]   = df["timestamp"].dt.day
    return df

def extract_all(day: date | None = None) -> pd.DataFrame:
    frames = []
    for loc in _load_locations():
        df = _fetch_open_meteo(loc["lat"], loc["lon"], day=day)
        df["location"] = loc["name"]   # <-- crucial: stamp the city
        frames.append(df)
    return pd.concat(frames, ignore_index=True)
