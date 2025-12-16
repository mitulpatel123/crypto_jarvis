import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import config
import logging

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [PatternRec] %(message)s')
logger = logging.getLogger("PatternRec")

class PatternRecognizer:
    def __init__(self):
        self.engine = create_engine(config.DB_URI)

    def fetch_ohlcv(self, symbol="btcusdt", limit=100):
        query = f"""
            SELECT time, price, volume 
            FROM market_ticks 
            WHERE symbol = '{symbol}' 
            AND source NOT LIKE '%%Book%%' 
            AND source NOT LIKE '%%Depth%%' 
            ORDER BY time DESC LIMIT {limit}
        """
        df = pd.read_sql(query, self.engine)
        if df.empty:
             return None
        
        # Rename price -> close for resampling
        df = df.rename(columns={'price': 'close'})
        
        
        # Resample to 1-minute OHLCV (Ticks are raw trades)
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        ohlc = df['price'].resample('1Min').ohlc()
        ohlc['volume'] = df['volume'].resample('1Min').sum()
        return ohlc.dropna()

    def detect_patterns(self, df):
        """
        Detects basic patterns without heavy dependencies.
        Returns a list of detected patterns for the latest candle.
        """
        if len(df) < 5: return []
        
        patterns = []
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        body_size = abs(curr['close'] - curr['open'])
        wick_upper = curr['high'] - max(curr['close'], curr['open'])
        wick_lower = min(curr['close'], curr['open']) - curr['low']
        avg_body = (abs(df['close'] - df['open']).mean())
        
        # 1. Doji (Body is very small)
        if body_size <= avg_body * 0.1:
            patterns.append("Doji")
            
        # 2. Hammer (Small body, long lower wick)
        if wick_lower > body_size * 2 and wick_upper < body_size * 0.5:
             patterns.append("Hammer")
             
        # 3. Shooting Star (Small body, long upper wick)
        if wick_upper > body_size * 2 and wick_lower < body_size * 0.5:
             patterns.append("Shooting Star")
             
        # 4. Bullish Engulfing
        if (prev['close'] < prev['open']) and (curr['close'] > curr['open']):
            if curr['close'] > prev['open'] and curr['open'] < prev['close']:
                patterns.append("Bullish Engulfing")

        # 5. Bearish Engulfing
        if (prev['close'] > prev['open']) and (curr['close'] < curr['open']):
            if curr['open'] > prev['close'] and curr['close'] < prev['open']:
                patterns.append("Bearish Engulfing")
                
        return patterns

    def run_scan(self, symbol="btcusdt"):
        df = self.fetch_ohlcv(symbol)
        if df is not None:
            patterns = self.detect_patterns(df)
            if patterns:
                logger.info(f"Detected Patterns for {symbol}: {patterns}")
                return patterns
        return []

if __name__ == "__main__":
    pr = PatternRecognizer()
    pr.run_scan()
