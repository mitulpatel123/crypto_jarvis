import psycopg2
import pandas as pd
import streamlit as st
import time

import warnings
warnings.filterwarnings('ignore')

# DB Configuration
DB_URI = "postgres://postgres:password@127.0.0.1:5433/postgres"

@st.cache_resource
def get_connection():
    """Establishes a persistent connection to TimescaleDB."""
    try:
        conn = psycopg2.connect(DB_URI)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to DB: {e}")
        return None

def fetch_ticks(seconds=60):
    """Fetch aggregated ticks for Price Charts."""
    query = f"""
    SELECT time_bucket('1 second', time) AS bucket, 
           symbol, 
           last(price, time) as close_price, 
           sum(volume) as volume
    FROM market_ticks 
    WHERE time > NOW() - INTERVAL '{seconds} seconds'
    GROUP BY bucket, symbol 
    ORDER BY bucket DESC;
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except: return pd.DataFrame()
    return pd.DataFrame()

def fetch_cvd(seconds=300):
    """Fetch Buying vs Selling Volume for CVD Calculation."""
    query = f"""
    SELECT time_bucket('5 seconds', time) AS bucket, 
           symbol,
           SUM(CASE WHEN side = 'BUY' THEN volume ELSE 0 END) as buy_vol,
           SUM(CASE WHEN side = 'SELL' THEN volume ELSE 0 END) as sell_vol
    FROM market_ticks
    WHERE time > NOW() - INTERVAL '{seconds} seconds'
    GROUP BY bucket, symbol
    ORDER BY bucket ASC;
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except: return pd.DataFrame()
    return pd.DataFrame()

def fetch_derivatives():
    """Fetch latest Funding Rates and Options Data."""
    query = """
    SELECT symbol, funding_rate, open_interest, iv, delta, gamma, expiry, strike, option_type
    FROM derivatives_stats
    WHERE time > NOW() - INTERVAL '5 minutes'
    ORDER BY time DESC
    LIMIT 100;
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except: return pd.DataFrame()
    return pd.DataFrame()

def fetch_news():
    """Fetch latest News and Whale Alerts."""
    query = """
    SELECT time, source, title, sentiment, currency, amount 
    FROM news_sentiment 
    ORDER BY time DESC 
    LIMIT 20;
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except: return pd.DataFrame()
    return pd.DataFrame()

def get_system_metrics():
    """Get rows inserted in the last minute."""
    query = "SELECT count(*) FROM market_ticks WHERE time > NOW() - INTERVAL '1 minute';"
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()[0]
        except: return 0
    return 0
