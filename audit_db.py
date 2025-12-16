import psycopg2
import pandas as pd
import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB_Audit")

def audit_data():
    try:
        conn = psycopg2.connect(config.DB_URI)
        
        # 1. Check Market Ticks (Latest 100)
        logger.info("--- Auditing Market Ticks ---")
        df_ticks = pd.read_sql("SELECT * FROM market_ticks ORDER BY time DESC LIMIT 100", conn)
        
        if df_ticks.empty:
            logger.warning("Market Ticks Table is EMPTY!")
        else:
            logger.info(f"Loaded {len(df_ticks)} rows.")
            
            # Check for Zeros in Price/Volume
            zeros_price = df_ticks[df_ticks['price'] == 0]
            zeros_vol = df_ticks[df_ticks['volume'] == 0]
            
            logger.info(f"Rows with Price=0: {len(zeros_price)}")
            logger.info(f"Rows with Volume=0: {len(zeros_vol)}")
            
            if not zeros_vol.empty:
                logger.info("Sample of Volume=0 rows (Source analysis):")
                print(zeros_vol[['source', 'symbol', 'volume']].head(5))

        # 2. Check Features
        logger.info("\n--- Auditing Market Features ---")
        df_feat = pd.read_sql("SELECT * FROM market_features ORDER BY time DESC LIMIT 50", conn)
        if df_feat.empty:
            logger.warning("Market Features Table is EMPTY! (Feature Engine might be lagging)")
        else:
             print(df_feat.head())

    except Exception as e:
        logger.error(f"Audit Failed: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    audit_data()
