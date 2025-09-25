import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Stock Price with News Sentiment",
    layout="wide"  # This makes the app use the full browser width
)

# --- Initialize BigQuery client with Streamlit secrets ---
gcp_info = st.secrets["gcp"]
credentials = service_account.Credentials.from_service_account_info(gcp_info)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# --- Load data from BigQuery ---
@st.cache_data(ttl=3600)
def load_data():
    df_stock = bq_client.query("""
        SELECT * FROM `project-portfolio-473015.stock_data_append.stock_daily`
    """).to_dataframe()
    
    df_news = bq_client.query("""
        SELECT * FROM `project-portfolio-473015.stock_data_append.stock_news`
    """).to_dataframe()
    
    # Preprocess news
    df_news = df_news.drop_duplicates(subset=['id']).reset_index(drop=True)
    news_expanded = df_news.explode('insights').reset_index(drop=True)
    news_expanded['sentiment'] = news_expanded['insights'].apply(lambda x: x.get('sentiment') if isinstance(x, dict) else np.nan)
    news_expanded['ticker'] = news_expanded['insights'].apply(lambda x: x.get('ticker') if isinstance(x, dict) else np.nan)
    news_expanded = news_expanded.dropna(subset=['sentiment','ticker'])
    news_expanded['date'] = pd.to_datetime(news_expanded['published_utc']).dt.date

    # Preprocess stock
    df_stock['ts'] = pd.to_datetime(df_stock['ts'])
    df_stock['date'] = df_stock['ts'].dt.date

    # Compute daily sentiment scores
    daily_sentiment = news_expanded.groupby(['ticker','date'])['sentiment'].apply(list).reset_index()
    
    def sentiment_score(lst):
        return sum(1 if s=='positive' else -1 if s=='negative' else 0 for s in lst)

    daily_sentiment['sentiment_score'] = daily_sentiment['sentiment'].apply(sentiment_score)
    daily_sentiment['news_count'] = daily_sentiment['sentiment'].apply(len)

    # Merge stock and sentiment
    merged = pd.merge(df_stock, daily_sentiment, left_on=['symbol','date'], right_on=['ticker','date'], how='left')
    merged['sentiment_score'] = merged['sentiment_score'].fillna(0)
    merged['news_count'] = merged['news_count'].fillna(0)

    return merged, news_expanded

merged, news_expanded = load_data()

# --- Streamlit UI ---
st.title("Stock Price with News Sentiment")

# Select ticker
available_tickers = merged['symbol'].unique()
selected_ticker = st.selectbox("Select Ticker", available_tickers)

df_ticker = merged[merged['symbol']==selected_ticker]

# --- Plot candlestick + sentiment ---
fig = go.Figure(data=[go.Candlestick(
    x=df_ticker['ts'],
    open=df_ticker['open'],
    high=df_ticker['high'],
    low=df_ticker['low'],
    close=df_ticker['close'],
    name='Price'
)])

# Add sentiment markers
fig.add_trace(go.Scatter(
    x=df_ticker['ts'],
    y=df_ticker['close'] + 2,
    mode='markers',
    marker=dict(
        size=10,
        color=['green' if s>0 else 'red' if s<0 else 'gray' for s in df_ticker['sentiment_score']],
        symbol='diamond'
    ),
    hovertext=[f"Sentiment Score: {s}<br>News Count: {c}" 
               for s, c in zip(df_ticker['sentiment_score'], df_ticker['news_count'])],
    name='News Sentiment'
))

fig.update_layout(
    title=f'{selected_ticker} Daily Price with Sentiment',
    xaxis_title='Date',
    yaxis_title='Price',
    xaxis_rangeslider_visible=False,
    hovermode='x unified',
     font=dict(
        family="Segoe UI, sans-serif",  # Standardize font
        size=12,
        color="black"
    )
)

# Render chart
st.plotly_chart(fig, use_container_width=True)

# --- Clickable news table ---
st.subheader("News Details")
clicked_date = st.date_input("Select Date", value=df_ticker['date'].max())

day_news = news_expanded[(news_expanded['date']==clicked_date) & (news_expanded['ticker']==selected_ticker)]

if day_news.empty:
    st.info(f"No news found for {selected_ticker} on {clicked_date}")
else:
    # Build HTML table
    table_html = '<table style=width:100%; border:1px solid black; border-collapse:collapse; font-family:Segoe UI, sans-serif;>'
    table_html += """
    <tr>
        <th style="border: 1px solid black; padding: 4px;">Ticker</th>
        <th style="border: 1px solid black; padding: 4px;">Headline</th>
        <th style="border: 1px solid black; padding: 4px;">Sentiment</th>
        <th style="border: 1px solid black; padding: 4px;">Link</th>
    </tr>
    """

    for _, row in day_news.iterrows():
        sentiment_color = "green" if row['sentiment']=='positive' else "red" if row['sentiment']=='negative' else "gray"
        link = f'<a href="{row.get("article_url","#")}" target="_blank">Link</a>'
        table_html += f"""
        <tr>
            <td style="border: 1px solid black; padding: 4px;">{row['ticker']}</td>
            <td style="border: 1px solid black; padding: 4px;">{row['title']}</td>
            <td style="border: 1px solid black; padding: 4px; color:{sentiment_color};">{row['sentiment']}</td>
            <td style="border: 1px solid black; padding: 4px;">{link}</td>
        </tr>
        """
    table_html += "</table>"

    # Render using components.html for full HTML support
    components.html(table_html, width="100%", height=400, scrolling=True)