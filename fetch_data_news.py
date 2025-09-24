import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# -----------------------------
# Config
# -----------------------------
PROJECT_ID = "project-portfolio-473015"      # 🔹 replace with your GCP project
DATASET = "stock_data_append"
TABLE = f"{PROJECT_ID}.{DATASET}.stock_news"

SYMBOL = "NFLX"
POLYGON_API_KEY = "s_po7wmfS3zeKBzcL0D2wdkv2H7Z6RSG"  # 🔹 replace for testing

# -----------------------------
# BigQuery Client
# -----------------------------
bq_client = bigquery.Client(project=PROJECT_ID)

# -----------------------------
# Fetch stock news
# -----------------------------
def fetch_stock_news():
    start_date = datetime(2025, 7, 25).strftime("%Y-%m-%d")
    end_date = datetime(2025, 9, 24).strftime("%Y-%m-%d")

    url = (
        f"https://api.polygon.io/v2/reference/news"
        f"?ticker={SYMBOL}&published_utc.gte={start_date}"
        f"&published_utc.lte={end_date}&limit=1000&apiKey={POLYGON_API_KEY}"
    )
    resp = requests.get(url).json()

    if "results" not in resp:
        print("❌ API error:", resp)
        return pd.DataFrame()

    articles = resp["results"]
    print(f"✅ Fetched {len(articles)} articles for {SYMBOL}")

    rows = []
    for a in articles:
        rows.append({
            "id": a.get("id"),
            "title": a.get("title"),
            "author": a.get("author"),
            "description": a.get("description"),
            "article_url": a.get("article_url"),
            "amp_url": a.get("amp_url"),
            "image_url": a.get("image_url"),
            "published_utc": a.get("published_utc"),
            "publisher": {
                "name": a.get("publisher", {}).get("name"),
                "homepage_url": a.get("publisher", {}).get("homepage_url"),
                "logo_url": a.get("publisher", {}).get("logo_url"),
                "favicon_url": a.get("publisher", {}).get("favicon_url")
            },
            "tickers": a.get("tickers", []),
            "keywords": a.get("keywords", []),
            "insights": a.get("insights", [])
        })

    return pd.DataFrame(rows)

# -----------------------------
# Load into BigQuery (batch load)
# -----------------------------
def load_to_bigquery(df: pd.DataFrame):
    if df.empty:
        print("⚠️ No news rows to load")
        return

# Convert published_utc to datetime (UTC)
    if "published_utc" in df.columns:
        df["published_utc"] = pd.to_datetime(df["published_utc"], errors="coerce", utc=True)
        
    job = bq_client.load_table_from_dataframe(df, TABLE)
    job.result()  # Wait for job to finish

    print(f"✅ Loaded {len(df)} news rows into {TABLE}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    df_news = fetch_stock_news()
    load_to_bigquery(df_news)
