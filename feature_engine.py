import time
import json
import logging
import pandas as pd
import pandas_ta as ta  # Technical Analysis Lib
import psycopg2
from datetime import datetime, timezone
import config

from sqlalchemy import create_engine

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [FeatureEngine] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/feature_engine.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FeatureEngine")

class FeatureEngine:
    def __init__(self):
        # SQLAlchemy Engine for Pandas (Read)
        self.engine = create_engine(config.DB_URI.replace("postgres://", "postgresql://"))
        
        # Psycopg2 Connection for Writing (Upsert is cleaner in SQL)
        self.conn = psycopg2.connect(config.DB_URI)
        self.conn.autocommit = True
        
        logger.info("Connected to DB (SQLAlchemy + Psycopg2)")

    def fetch_recent_data(self, symbol, limit=500):
        """Fetch OHLVC data for calculation"""
        query = f"""
            SELECT time, price, volume, side 
            FROM market_ticks 
            WHERE (symbol = '{symbol}' OR symbol = '{symbol.upper()}')
            AND time > NOW() - INTERVAL '1 hour'
            ORDER BY time ASC;
        """
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                df = df.sort_values(by='time').reset_index(drop=True)
                
                # Resample to 1-minute candles if raw ticks
                df['time'] = pd.to_datetime(df['time'])
                df = df.set_index('time')
                
                # Tick to Candle
                ohlc = df['price'].resample('1min').ohlc()
                volume = df['volume'].resample('1min').sum()
                df_candle = pd.concat([ohlc, volume], axis=1)
                
                # Drop NaNs from gaps
                df_candle = df_candle.dropna()
                
                # Restore 'close' column name for pandas_ta
                return df_candle
                
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Fetch Error: {e}")
            return pd.DataFrame()

    def calculate_momentum(self, df):
        """RSI, MACD, Bollinger Bands"""
        if len(df) < 50: return {}
        
        # Calculate RSI
        try:
             df['rsi'] = ta.rsi(df['close'], length=14)
        except: df['rsi'] = pd.Series([0]*len(df))
        
        # Calculate MACD
        try:
             macd = ta.macd(df['close']) # Returns MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9
        except: macd = pd.DataFrame({'MACD_12_26_9': [0]*len(df), 'MACDs_12_26_9': [0]*len(df)})
        
        # NaN safe get
        def safe_float(val):
             if pd.isna(val): return 0.0
             return float(val)

        # Latest values
        return {
            "rsi": safe_float(df['rsi'].iloc[-1]),
            "macd": safe_float(macd['MACD_12_26_9'].iloc[-1]),
            "macd_signal": safe_float(macd['MACDs_12_26_9'].iloc[-1]),
            "price": safe_float(df['close'].iloc[-1])
        }

    def calculate_orderflow(self, df):
        """Calculate Cumulative Volume Delta (CVD)"""
        if df.empty or 'side' not in df.columns: return {}
        
        try:
            # Filter valid trades
            trades = df[df['side'].isin(['BUY', 'SELL', 'Buy', 'Sell', 'buy', 'sell'])]
            
            if trades.empty: return {"cvd_current": 0.0, "cvd_slope": 0.0}
            
            # Vectorized calculation
            # Assign +Vol for BUY, -Vol for SELL
            trades = trades.copy()
            trades['signed_vol'] = trades.apply(lambda x: x['volume'] if str(x['side']).upper() == 'BUY' else -x['volume'], axis=1)
            
            # Cumulative Sum
            trades['cvd'] = trades['signed_vol'].cumsum()
            
            # Calculate Slope (Change over last 10 points)
            if len(trades) > 10:
                slope = trades['cvd'].iloc[-1] - trades['cvd'].iloc[-10]
            else:
                slope = 0.0
                
            return {
                "cvd_current": float(trades['cvd'].iloc[-1]),
                "cvd_slope": float(slope),
                "volume_buy": float(trades[trades['signed_vol'] > 0]['volume'].sum()),
                "volume_sell": float(trades[trades['signed_vol'] < 0]['volume'].sum())
            }
        except Exception as e:
            logger.error(f"CVD Error: {e}")
            return {} 

    def calculate_patterns(self, df):
        """Candlestick Pattern Recognition"""
        if len(df) < 5: return {}
        
        patterns = {}
        try:
            # Detect Doji
            doji = ta.cdl_doji(df['open'], df['high'], df['low'], df['close'])
            if doji is not None and not doji.empty and doji.iloc[-1] != 0: patterns['doji'] = True
            
            # Detect Engulfing (Note: requires open/high/low/close Series)
            # pandas_ta likely exposes it as 'cdl_engulfing' OR via strategy. 
            # If standard call fails, use lower level or skip.
            # Verified: 'cdl_engulfing' is not directly exposed in top-level 'ta'?
            # Let's try df.ta.cdl_pattern(name="engulfing") which abstracts it.
            # But let's check one more time if straight 'engulfing' works.
            # For robustness, we will try/except specific calls.
            
            # Trying df.ta accessor approach which is safer
            # Note: df must have valid index and columns
            
            if hasattr(ta, 'cdl_engulfing'):
                engulfing = ta.cdl_engulfing(df['open'], df['high'], df['low'], df['close'])
                if engulfing is not None and not engulfing.empty and engulfing.iloc[-1] != 0: 
                    patterns['engulfing'] = "BULL" if engulfing.iloc[-1] > 0 else "BEAR"
            
            # Hammer
            if hasattr(ta, 'cdl_hammer'):
                hammer = ta.cdl_hammer(df['open'], df['high'], df['low'], df['close'])
                if hammer is not None and not hammer.empty and hammer.iloc[-1] != 0: patterns['hammer'] = True

        except Exception as e:
            logger.error(f"Pattern Error: {e}")
            
        return patterns

    def save_features(self, symbol, group, data):
        """Store computed features for RL Agent"""
        if not data: return
        
        # Use simple Upsert
        # Note: JSONB allows flexible schema for the AI to explore new features later
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO market_features (time, symbol, feature_group, feature_data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (time, symbol, feature_group) DO UPDATE 
                    SET feature_data = EXCLUDED.feature_data;
                """, (datetime.now(timezone.utc), symbol, group, json.dumps(data)))
            except Exception as e:
                logger.error(f"Save Error: {e}")

    def run_loop(self):
        symbols = ["btcusdt", "ethusdt", "solusdt", "BTC-PERPETUAL", "ETH-PERPETUAL", "SOL-PERPETUAL"] 
        # Note: Symbols in DB might be different case or name, check market_ticks distinct symbol
        
        logger.info("Starting Feature Engine Loop...")
        while True:
            for sym in symbols:
                # 1. Fetch
                df = self.fetch_recent_data(sym)
                if df.empty: continue

                # 2. Calculate
                momentum = self.calculate_momentum(df)
                patterns = self.calculate_patterns(df)
                
                # Merge patterns into momentum for now or save separate
                if patterns: momentum['patterns'] = patterns

                # 3. Save
                self.save_features(sym, "momentum", momentum)
                
                # Orderflow logic
                orderflow = self.calculate_orderflow(df)
                if orderflow:
                    self.save_features(sym, "orderflow", orderflow)
            
            # 4. Sleep (Reduced frequency for CPU conservation)
            time.sleep(15.0)

if __name__ == "__main__":
    try:
        engine = FeatureEngine()
        engine.run_loop()
    except KeyboardInterrupt:
        pass
