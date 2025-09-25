import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from streamlit_plotly_events import plotly_events

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
    
    df_news = df_news.drop_duplicates(subset=['id']).reset_index(drop=True)
    news_expanded = df_news.explode('insights').reset_index(drop=True)
    news_expanded['sentiment'] = news_expanded['insights'].apply(lambda x: x.get('sentiment') if isinstance(x, dict) else np.nan)
    news_expanded['ticker'] = news_expanded['insights'].apply(lambda x: x.get('ticker') if isinstance(x, dict) else np.nan)
    news_expanded = news_expanded.dropna(subset=['sentiment','ticker'])
    news_expanded['date'] = pd.to_datetime(news_expanded['published_utc']).dt.date

    df_stock['ts'] = pd.to_datetime(df_stock['ts'])
    df_stock['date'] = df_stock['ts'].dt.date

    daily_sentiment = news_expanded.groupby(['ticker','date'])['sentiment'].apply(list).reset_index()
    
    def sentiment_score(lst):
        return sum(1 if s=='positive' else -1 if s=='negative' else 0 for s in lst)

    daily_sentiment['sentiment_score'] = daily_sentiment['sentiment'].apply(sentiment_score)
    daily_sentiment['news_count'] = daily_sentiment['sentiment'].apply(len)

    merged = pd.merge(df_stock, daily_sentiment, left_on=['symbol','date'], right_on=['ticker','date'], how='left')
    merged['sentiment_score'] = merged['sentiment_score'].fillna(0)
    merged['news_count'] = merged['news_count'].fillna(0)

    return merged, news_expanded

merged, news_expanded = load_data()

# --- Streamlit UI ---
st.title("Stock Price with News Sentiment")

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
    hovermode='x unified'
)

# --- Streamlit Plotly events ---
st.subheader("Candlestick Chart")
clicked_points = plotly_events(fig, click_event=True, hover_event=False)

# Determine the clicked date
if clicked_points:
    clicked_date = pd.to_datetime(clicked_points[0]['x']).date()
else:
    clicked_date = st.date_input("Select Date", value=df_ticker['date'].max())

# --- Display news for clicked date ---
st.subheader("News Details")
day_news = news_expanded[(news_expanded['date']==clicked_date) & (news_expanded['ticker']==selected_ticker)]

if day_news.empty:
    st.info(f"No news found for {selected_ticker} on {clicked_date}")
else:
    day_news_display = day_news[['ticker','title','sentiment','article_url']].copy()
    day_news_display['sentiment'] = day_news_display['sentiment'].map(
        lambda x: f"🟢 {x}" if x=='positive' else f"🔴 {x}" if x=='negative' else x
    )
    day_news_display['article_url'] = day_news_display['article_url'].apply(lambda x: f"[Link]({x})" if x else "#")
    st.dataframe(day_news_display, use_container_width=True)

