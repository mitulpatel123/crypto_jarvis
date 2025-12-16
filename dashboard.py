import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import dashboard_db as db

st.set_page_config(page_title="Crypto Real-Time Analytics", layout="wide", page_icon="⚡")

# Auto-refresh every 5 seconds (more stable)
count = st_autorefresh(interval=5000, limit=None, key="fizzbuzz")

# Sidebar - System Health
st.sidebar.title("⚡ Data Pipeline")
ticks_last_min = db.get_system_metrics()
st.sidebar.metric("Ingestion Rate", f"{ticks_last_min} ticks/min")
st.sidebar.success("TimescaleDB Connected (Port 5433)")

# Title
st.title("Crypto Algo-Trading Control Center")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📈 Price Action", "🌊 Order Flow (CVD)", "📊 Derivatives & Greeks", "📰 News & Whales"])

with tab1:
    st.subheader("Live Market Data (1-Sec Aggregation)")
    df_ticks = db.fetch_ticks(seconds=60)
    
    if not df_ticks.empty:
        # Create multi-line chart for symbols
        fig = px.line(df_ticks, x='bucket', y='close_price', color='symbol', title="Real-Time Price Action (Last 60s)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Waiting for data...")

with tab2:
    st.subheader("Cumulative Volume Delta (CVD)")
    st.info("Visualizes Aggressive Buying vs Selling Pressure (Phase 4.5 Feature)")
    
    df_cvd = db.fetch_cvd(seconds=300)
    if not df_cvd.empty:
        # Calculate Delta
        df_cvd['delta'] = df_cvd['buy_vol'] - df_cvd['sell_vol']
        # Cumulative sum per symbol
        df_cvd['cvd'] = df_cvd.groupby('symbol')['delta'].cumsum()
        
        fig_cvd = px.line(df_cvd, x='bucket', y='cvd', color='symbol', title="CVD Trend (Last 5 Mins)")
        st.plotly_chart(fig_cvd, use_container_width=True)
        
        # Bar chart for Net Flow
        fig_flow = px.bar(df_cvd, x='bucket', y='delta', color='symbol', title="Net Order Flow per 5s Bucket")
        st.plotly_chart(fig_flow, use_container_width=True)
    else:
        st.warning("Insufficient data for CVD calculation.")

with tab3:
    st.subheader("Derivatives Intelligence")
    col1, col2 = st.columns(2)
    
    df_derivs = db.fetch_derivatives()
    if not df_derivs.empty:
        with col1:
            st.markdown("### Funding Rates & Open Interest")
            # Show if ANY interesting field is present, don't dropna row-wise
            cols = ['symbol', 'funding_rate', 'open_interest']
            st.dataframe(df_derivs[cols].dropna(subset=['funding_rate', 'open_interest'], how='all').head(20))
        
        with col2:
            st.markdown("### Options Flow (Greeks)")
            # Filter for options only
            df_options = df_derivs[df_derivs['option_type'].notna()]
            if not df_options.empty:
                st.dataframe(df_options[['symbol', 'expiry', 'strike', 'option_type', 'iv', 'delta', 'gamma']])
            else:
                st.info("No Options data captured yet.")
    else:
        st.warning("No derivatives data.")

with tab4:
    col_news, col_whale = st.columns(2)
    
    df_news = db.fetch_news()
    if not df_news.empty:
        with col_news:
            st.subheader("📰 CryptoPanic Headlines")
            headlines = df_news[df_news['source'] == 'CryptoPanic']
            for index, row in headlines.iterrows():
                sentiment_emoji = "🟢" if row['sentiment'] == 'Bullish' else "🔴" if row['sentiment'] == 'Bearish' else "⚪"
                st.markdown(f"**{row['time'].strftime('%H:%M:%S')}** {sentiment_emoji} [{row['title']}](#)")
        
        with col_whale:
            st.subheader("🐋 Whale Alerts")
            whales = df_news[df_news['source'].str.contains('Whale')]
            st.dataframe(whales[['time', 'currency', 'amount']])
    else:
        st.info("No news or whale alerts yet.")
