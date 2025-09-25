import os, json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import Dash
from dash import html
from dash.dependencies import Input, Output
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
from streamlit.components.v1 import iframe

# Load the service account info from Streamlit secrets
gcp_info = st.secrets["gcp"]

credentials = service_account.Credentials.from_service_account_info(gcp_info)

bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Read credentials JSON from env var
# creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
# credentials = service_account.Credentials.from_service_account_info(creds_dict)

# Init BigQuery client
# bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Projects\Profile\Polygon_project\Stock-Market-Analysis-with-News-Sentiment-Overlay\project-portfolio-473015-eedb2f040835.json"
# bq_client = bigquery.Client()

# Load stock data

df_stock = bq_client.query("""
    SELECT * FROM `project-portfolio-473015.stock_data_append.stock_daily`
""").to_dataframe()

# Load news data
df_news = bq_client.query("""
    SELECT * FROM `project-portfolio-473015.stock_data_append.stock_news`
""").to_dataframe()

df_news = df_news.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)

# Assume your original news dataframe is df_news
# Explode the 'insights' list into separate rows
news_expanded = df_news.explode('insights').reset_index(drop=True)

# Extract ticker and sentiment safely
news_expanded['sentiment'] = news_expanded['insights'].apply(lambda x: x.get('sentiment') if isinstance(x, dict) else np.nan)
news_expanded['ticker'] = news_expanded['insights'].apply(lambda x: x.get('ticker') if isinstance(x, dict) else np.nan)

# Drop rows with missing sentiment or ticker
news_expanded = news_expanded.dropna(subset=['sentiment', 'ticker'])

# Add 'date' column if not already present
news_expanded['date'] = pd.to_datetime(news_expanded['published_utc']).dt.date

# Make sure df_stock['ts'] is datetime
df_stock['ts'] = pd.to_datetime(df_stock['ts'])
df_stock['date'] = df_stock['ts'].dt.date

# Compute daily sentiment score for each ticker
daily_sentiment = news_expanded.groupby(['ticker','date'])['sentiment'].apply(list).reset_index()

def sentiment_score(lst):
    score = 0
    for s in lst:
        if s == 'positive':
            score += 1
        elif s == 'negative':
            score -= 1
    return score

daily_sentiment['sentiment_score'] = daily_sentiment['sentiment'].apply(sentiment_score)
daily_sentiment['news_count'] = daily_sentiment['sentiment'].apply(len)

# Merge with stock data
merged = pd.merge(df_stock, daily_sentiment, left_on=['symbol','date'], right_on=['ticker','date'], how='left')
merged['sentiment_score'] = merged['sentiment_score'].fillna(0)
merged['news_count'] = merged['news_count'].fillna(0)


# --- Initialize Dash app ---
app = Dash(__name__)

# Expose the server for deployment
server = app.server   

# Get unique tickers from the stock data
available_tickers = df_stock['symbol'].unique()

# Dropdown options based on available stock tickers
tickers_options = [{'label': t, 'value': t} for t in available_tickers]

app.layout = html.Div([
    html.H2("Stock Price with News Sentiment"),
    
    html.Div([
        html.Label("Select Ticker:"),
        dcc.Dropdown(
            id='ticker-filter',
            options=tickers_options,
            value=available_tickers[0],  # default selection
            clearable=False
        )
    ], style={'width': '200px', 'margin-bottom': '20px'}),
    
    dcc.Graph(id='candlestick-chart'),
    html.H4("News Details"),
    html.Div(id='news-table')
])

# --- Callback to update chart based on selected ticker ---
@app.callback(
    Output('candlestick-chart', 'figure'),
    Input('ticker-filter', 'value')
)
def update_chart(selected_ticker):
    df_stock_ticker = merged[merged['symbol'] == selected_ticker]
    
    fig = go.Figure(data=[go.Candlestick(
        x=df_stock_ticker['ts'],
        open=df_stock_ticker['open'],
        high=df_stock_ticker['high'],
        low=df_stock_ticker['low'],
        close=df_stock_ticker['close'],
        name='Price'
    )])
    
    # Filter sentiment for selected ticker
    daily_sentiment_ticker = df_stock_ticker[['ts','close','sentiment_score','news_count']]
    
    # Add sentiment markers
    fig.add_trace(go.Scatter(
        x=daily_sentiment_ticker['ts'],
        y=daily_sentiment_ticker['close'] + 2,
        mode='markers',
        marker=dict(
            size=10,
            color=['green' if s > 0 else 'red' if s < 0 else 'gray' 
                   for s in daily_sentiment_ticker['sentiment_score']],
            symbol='diamond'
        ),
        hovertext=[
            f"Sentiment Score: {s}<br>News Count: {c}"
            for s, c in zip(daily_sentiment_ticker['sentiment_score'], daily_sentiment_ticker['news_count'])
        ],
        name='News Sentiment'
    ))
    
    fig.update_layout(
        title=f'{selected_ticker} Daily Price with Sentiment',
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    return fig

# --- Callback to display news table on marker click ---
@app.callback(
    Output('news-table', 'children'),
    [Input('candlestick-chart', 'clickData'),
     Input('ticker-filter', 'value')]
)
def display_news(clickData, selected_ticker):
    if clickData is None:
        return "Click on a marker to see news details for that day."
    
    clicked_date = pd.to_datetime(clickData['points'][0]['x']).date()
    
    day_news = news_expanded[
        (news_expanded['date'] == clicked_date) &
        (news_expanded['ticker'] == selected_ticker)
    ]
    
    if day_news.empty:
        return f"No news found for {selected_ticker} on {clicked_date}"
    
    # Create a styled table
    table = html.Table([
        html.Tr([html.Th("Ticker"), html.Th("Headline"), html.Th("Sentiment"), html.Th("Link")])] +
        [
            html.Tr([
                html.Td(row['ticker']),
                html.Td(row['title']),
                html.Td(row['sentiment'], style={'color': 'green' if row['sentiment']=='positive' else 'red' if row['sentiment']=='negative' else 'gray'}),
                html.Td(html.A("Link", href=row.get('article_url', '#'), target="_blank"))
            ]) for _, row in day_news.iterrows()
        ],
        style={'width': '100%', 'border': '1px solid black', 'border-collapse': 'collapse'}
    )
    
    return table

# --- Run the Dash app ---
if __name__ == "__main__":
    st.components.v1.html(
    '<iframe srcdoc="{0}" width="100%" height="600"></iframe>'.format(
        app.index()
    ),
    height=600,
)
    # app.run_server(host="0.0.0.0", port=8080, debug=True)
    # app.run_server(debug=True)
