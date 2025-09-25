# Project Title: Stock Market Analysis with News Sentiment Overlay

## Overview

This project focuses on analyzing the daily price trends of top stocks by market capitalization along with related news articles. The aim is to explore how news sentiment impacts stock prices and provide an interactive visualization for deeper insights.


Data Source: [Polygon.io](https://polygon.io/) REST API

Plotly Dash app on render: https://stock-market-analysis-with-news-sentiment.onrender.com

## Data Collected:

1. Stock Price Data: Daily OHLC (Open, High, Low, Close), volume, VWAP, and trades count for selected top market-value stocks (e.g., NVDA, MSFT, AAPL).

2. News Data: Related news articles including title, author, description, URL, publisher, published timestamp, and sentiment insights per ticker.

3. Time Range: Historical data collected for the last 6 months with plans for monthly ingestion to keep the dataset updated.


## Data Ingestion

Database: Google BigQuery


## Tables Created:

1. stock_daily → Contains daily OHLC and trading volume for each ticker.

2. stock_news → Contains news articles along with structured insights and sentiment data.

Method: Python scripts used to fetch data via Polygon API and load it into BigQuery tables.



## Data Processing & EDA

Data loaded into Python using Pandas for cleaning, transformation, and exploratory data analysis.


## EDA Highlights:

1.Checked for duplicate entries in both stock and news datasets.

2.Computed derived metrics such as daily returns, rolling 7-day and 30-day volatility.

3. Analyzed sentiment distribution across news articles (positive, neutral, negative).

4. Correlation analysis between daily sentiment scores and stock returns to detect possible impact of news on price movement.


## Visualization

1. Interactive Plotly Dash app deployed on Render.

2. Displays candlestick charts for stock price trends.

3. Overlay sentiment markers for each day with corresponding positive or negative news.
   
4. Clickable chart markers which filter down related news.

5. Hover tooltips show sentiment scores for quick inspection.

6. Interactive filter to select specific tickers for comparison.

7. News details table below the chart shows article information with hyperlinks.



## Key Features:

1. Drill-down capability for news on specific days.

2. Dynamic ticker filtering for multiple stock analysis.

3. Easy integration with updated monthly datasets for continuous monitoring.



## Skills & Tools Highlighted

1. Data Acquisition: REST API integration, Polygon.io.

2. Data Engineering: BigQuery ingestion and schema design, handling structured and nested JSON.

3. Data Analysis: Python, Pandas, NumPy, time-series analysis, correlation metrics.

4. Data Visualization & Dashboards: Plotly, Plotly Dash, interactive candlestick charts, sentiment markers.

5. Project Workflow: End-to-end pipeline from live data ingestion → database storage → Python EDA → interactive dashboard.



## Future Enhancements

1. Automate monthly ingestion using a scheduler or cloud function.

2. Add real-time data streaming with updates to BigQuery for near real-time dashboard insights.

Expand coverage to more tickers or indices.

Integrate advanced sentiment analysis using NLP models for finer-grained impact evaluation.
