import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

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

# --- Initialize Dash app ---
app = Dash(__name__)
server = app.server  # Required for deployment

# Dropdown options
available_tickers = merged['symbol'].unique()
tickers_options = [{'label': t, 'value': t} for t in available_tickers]

app.layout = html.Div([
    html.H2("Stock Price with News Sentiment"),
    html.Div([
        html.Label("Select Ticker:"),
        dcc.Dropdown(
            id='ticker-filter',
            options=tickers_options,
            value=available_tickers[0],
            clearable=False
        )
    ], style={'width': '200px', 'margin-bottom': '20px'}),
    dcc.Graph(id='candlestick-chart'),
    html.H4("News Details"),
    html.Div(id='news-table')
])

# --- Callbacks ---
@app.callback(
    Output('candlestick-chart', 'figure'),
    Input('ticker-filter', 'value')
)
def update_chart(selected_ticker):
    df_ticker = merged[merged['symbol']==selected_ticker]
    
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
        hovertext=[f"Sentiment: {s}<br>News: {c}" 
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
    return fig

@app.callback(
    Output('news-table', 'children'),
    [Input('candlestick-chart', 'clickData'),
     Input('ticker-filter', 'value')]
)
def display_news(clickData, selected_ticker):
    if clickData is None:
        return "Click on a marker to see news details for that day."
    
    clicked_date = pd.to_datetime(clickData['points'][0]['x']).date()
    day_news = news_expanded[(news_expanded['date']==clicked_date) & (news_expanded['ticker']==selected_ticker)]
    
    if day_news.empty:
        return f"No news found for {selected_ticker} on {clicked_date}"

    return html.Table([
        html.Tr([html.Th("Ticker"), html.Th("Headline"), html.Th("Sentiment"), html.Th("Link")])] +
        [html.Tr([
            html.Td(row['ticker']),
            html.Td(row['title']),
            html.Td(row['sentiment'], style={'color':'green' if row['sentiment']=='positive' else 'red' if row['sentiment']=='negative' else 'gray'}),
            html.Td(html.A("Link", href=row.get('article_url','#'), target="_blank"))
        ]) for _, row in day_news.iterrows()],
        style={'width':'100%','border':'1px solid black','border-collapse':'collapse'}
    )

# --- Render Dash app inside Streamlit ---
st.title("Stock Price & News Sentiment Dashboard")
st.components.v1.html(
    '<iframe srcdoc="{0}" width="100%" height="600"></iframe>'.format(app.index()),
    height=600,
)
