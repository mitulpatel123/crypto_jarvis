import pandas as pd
from sqlalchemy import create_engine
import config
import time

engine = create_engine(config.DB_URI)

print("--- DB VERIFICATION ---")
try:
    ticks = pd.read_sql("SELECT count(*) as c FROM market_ticks", engine).iloc[0]['c']
    news = pd.read_sql("SELECT count(*) as c FROM news_sentiment", engine).iloc[0]['c']
    derivs = pd.read_sql("SELECT count(*) as c FROM derivatives_stats", engine).iloc[0]['c']
    
    print(f"Ticks: {ticks}")
    print(f"News: {news}")
    print(f"Derivs: {derivs}")
    
    # Check recent
    recent_news = pd.read_sql("SELECT time, source, title FROM news_sentiment ORDER BY time DESC LIMIT 5", engine)
    print("\nRecent News:")
    print(recent_news)
    
except Exception as e:
    print(f"Error: {e}")
