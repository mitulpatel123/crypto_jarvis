import psycopg2
import pandas as pd
import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BTC_Audit")

def audit_btc():
    try:
        conn = psycopg2.connect(config.DB_URI)
        
        logger.info("--- Checking BTCUSDT Data ---")
        
        # Check specific trade sources
        query = """
            SELECT time, price, volume, source 
            FROM market_ticks 
            WHERE symbol='BTCUSDT' 
            AND source NOT LIKE '%%Book%%' 
            AND source NOT LIKE '%%Depth%%' 
            ORDER BY time DESC LIMIT 10
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("NO BTCUSDT TRADES FOUND! (Only Book/Depth might be present)")
        else:
            logger.info("Found BTCUSDT Trades:")
            print(df)
            
    except Exception as e:
        logger.error(f"Audit Failed: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    audit_btc()
