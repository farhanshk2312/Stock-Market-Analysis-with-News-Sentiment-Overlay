import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# -----------------------------
# Config
# -----------------------------
PROJECT_ID = "project-portfolio-473015"       # 🔹 replace with your project
DATASET = "stock_data_append"
TABLE = f"{PROJECT_ID}.{DATASET}.stock_daily"

SYMBOL = "NFLX"
POLYGON_API_KEY = "s_po7wmfS3zeKBzcL0D2wdkv2H7Z6RSG"  # 🔹 replace for testing

# -----------------------------
# BigQuery Client
# -----------------------------
bq_client = bigquery.Client(project=PROJECT_ID)

# ----------------------------- 
# Fetch stock data 
# -----------------------------
def fetch_stock_data():
    start_date = datetime(2025, 7, 25).strftime("%Y-%m-%d")
    end_date = datetime(2025, 9, 29).strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/ticker/{SYMBOL}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    resp = requests.get(url).json()

    if "results" not in resp:
        print("❌ API error:", resp)
        return pd.DataFrame()

    results = resp["results"]
    print(f"✅ Fetched {len(results)} rows for {SYMBOL}")

    rows = []
    for r in results:
        ts = datetime.utcfromtimestamp(r["t"] / 1000)  # convert ms → datetime
        rows.append({
            "symbol": SYMBOL,
            "ts": ts,                     # BigQuery TIMESTAMP column
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "close": r.get("c"),
            "volume": r.get("v"),
            "vwap": r.get("vw"),
            "trades_count": r.get("n")
        })

    return pd.DataFrame(rows)

# -----------------------------
# Load into BigQuery (batch load)
# -----------------------------
def load_to_bigquery(df: pd.DataFrame):
    if df.empty:
        print("⚠️ No rows to load")
        return

    job = bq_client.load_table_from_dataframe(df, TABLE)
    job.result()  # Wait for job to finish

    print(f"✅ Loaded {len(df)} rows into {TABLE}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    df = fetch_stock_data()
    load_to_bigquery(df)
