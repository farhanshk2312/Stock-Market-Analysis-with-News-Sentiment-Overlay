from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# Step 3a: Set your project and dataset
PROJECT = "project-portfolio-473015"   # replace with your project ID
DATASET = "stock_data_append"       # desired dataset name

# Step 3b: Initialize BigQuery client
client = bigquery.Client(project=PROJECT)

# Step 3c: Create dataset
def create_dataset():
    dataset_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset_ref.location = "US"  # choose location
    try:
        client.create_dataset(dataset_ref)
        print(f"Dataset {DATASET} created successfully!")
    except Conflict:
        print(f"Dataset {DATASET} already exists.")

# Step 3d: Create tables
def create_tables():
    # --- Table 1: stock_daily ---
    stock_daily_schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "FLOAT64"),
        bigquery.SchemaField("vwap", "FLOAT64"),
        bigquery.SchemaField("trades_count", "INT64")
    ]
    stock_daily_table_id = f"{PROJECT}.{DATASET}.stock_daily"
    stock_daily_table = bigquery.Table(stock_daily_table_id, schema=stock_daily_schema)
    
    # Partition by ts and cluster by symbol
    stock_daily_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ts"
    )
    stock_daily_table.clustering_fields = ["symbol"]

    # --- Table 2: stock_news ---
    stock_news_schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("author", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("article_url", "STRING"),
    bigquery.SchemaField("amp_url", "STRING"),
    bigquery.SchemaField("image_url", "STRING"),
    bigquery.SchemaField("published_utc", "TIMESTAMP"),
    bigquery.SchemaField(
        "publisher", "RECORD",
        fields=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("homepage_url", "STRING"),
            bigquery.SchemaField("logo_url", "STRING"),
            bigquery.SchemaField("favicon_url", "STRING"),
        ]
    ),
    bigquery.SchemaField("tickers", "STRING", mode="REPEATED"),
    bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "insights", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("sentiment", "STRING"),
            bigquery.SchemaField("sentiment_reasoning", "STRING")
        ]
    )
]
    
    stock_news_table_id = f"{PROJECT}.{DATASET}.stock_news"
    stock_news_table = bigquery.Table(stock_news_table_id, schema=stock_news_schema)
    
    # Partition by ts and cluster by symbol
    stock_news_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="published_utc"
    )
    stock_news_table.clustering_fields = ["id"]

    # Create tables
    for table in (stock_daily_table, stock_news_table):
        try:
            client.create_table(table)
            print(f"Table {table.table_id} created successfully!")
        except Conflict:
            print(f"Table {table.table_id} already exists.")

# Step 3e: Run creation
if __name__ == "__main__":
    create_dataset()
    create_tables()
