import streamlit as st
import pandas as pd
import json
import time
import config
from paper_exchange import PaperExchange
from pattern_recognition import PatternRecognizer
from sqlalchemy import create_engine
import plotly.graph_objects as go
import os

# --- Config ---
st.set_page_config(page_title="Crypto Jarvis Dashboard", layout="wide", page_icon="⚡")
st.title("⚡ Crypto Jarvis: Live Command Center")

# --- Auto-Refresh (Every 5 seconds) ---
if st.button("Refresh Data"):
    st.rerun()

# --- Database ---
engine = create_engine(config.DB_URI)

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs(["🚀 Live Market", "🧠 AI Council", "📜 Paper Trading", "📉 Patterns"])

# --- TAB 1: Live Market ---
with tab1:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Latest Ticks (Price)")
        try:
             df_ticks = pd.read_sql("SELECT * FROM market_ticks WHERE source NOT LIKE '%%Book%%' AND source NOT LIKE '%%Depth%%' ORDER BY time DESC LIMIT 20", engine)
             st.dataframe(df_ticks)
        except Exception as e:
             st.error(f"DB Error: {e}")
             
    with col2:
        st.subheader("News Sentiment")
        try:
             df_news = pd.read_sql("SELECT time, source, title, sentiment FROM news_sentiment ORDER BY time DESC LIMIT 10", engine)
             st.dataframe(df_news)
        except Exception as e:
             st.error(f"DB Error: {e}")

# --- TAB 2: AI Council ---
with tab2:
    st.header("The Council's War Room")
    # Read Logs
    log_path_1 = f"{config.LOG_DIR}/council.log"
    log_path_2 = f"{config.LOG_DIR}/council_ai.log"
    
    log_file = log_path_1 if os.path.exists(log_path_1) and os.path.getsize(log_path_1) > 0 else log_path_2
    
    try:
        with open(log_file, "r") as f:
            lines = f.readlines()
            
        # Parse for decisions
        decisions = []
        current_decision = ""
        capture = False
        for line in reversed(lines):
            if "--- COMMANDER DECISION ---" in line:
                capture = True
                continue
            if "---------------------------" in line and capture:
                capture = False
                decisions.append(current_decision)
                current_decision = ""
            if capture:
                current_decision = line + current_decision # Reverse append
                
        if decisions:
            st.info("Latest Commander Order:")
            st.markdown(decisions[0])
        else:
            st.warning("No Council Decisions found in logs yet.")
            
    except FileNotFoundError:
        st.error("Council Log not found. Is it running?")

# --- TAB 3: Paper Trading ---
with tab3:
    st.header("Paper Trading Portfolio (Virtual: $5k)")
    pe = PaperExchange()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Balance (USDT)", f"${pe.get_balance():.2f}")
    
    # Calculate Unrealized PnL (Mock Price for speed)
    current_btc = 90000.0
    try:
        last_tick = pd.read_sql("SELECT price FROM market_ticks WHERE symbol='btcusdt' ORDER BY time DESC LIMIT 1", engine)
        if not last_tick.empty:
            current_btc = float(last_tick.iloc[0]['price'])
    except: pass
    
    pnl, details = pe.get_live_pnl({'btcusdt': current_btc})
    col2.metric("Unrealized PnL", f"${pnl:.2f}", delta_color="normal")
    col3.metric("BTC Price Ref", f"${current_btc:.2f}")
    
    st.subheader("Open Positions")
    st.write(pe.portfolio["positions"])
    
    st.subheader("Trade History")
    st.dataframe(pd.DataFrame(pe.portfolio["history"]))

# --- TAB 4: Chart Patterns ---
with tab4:
    st.header("Detected Chart Patterns (OHLCV)")
    pr = PatternRecognizer()
    patterns = pr.run_scan("btcusdt")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        # Show mini chart
        df_ohlc = pr.fetch_ohlcv("btcusdt", limit=50)
        if df_ohlc is not None:
            fig = go.Figure(data=[go.Candlestick(x=df_ohlc.index,
                open=df_ohlc['open'], high=df_ohlc['high'],
                low=df_ohlc['low'], close=df_ohlc['close'])])
            st.plotly_chart(fig, use_container_width=True)
            
    with col2:
        st.subheader("Latest Patterns")
        if patterns:
             for p in patterns:
                 st.success(f"✅ {p}")
        else:
             st.info("No patterns detected in last candle.")

    st.caption("Pattern logic runs on 1-minute aggregations.")
