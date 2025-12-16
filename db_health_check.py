import pandas as pd
from sqlalchemy import create_engine
import config

def check_db():
    try:
        engine = create_engine(config.DB_URI)
        print("--- Database Health Check ---")
        
        tables = ['market_ticks', 'market_features', 'derivatives_stats', 'news_sentiment']
        
        for table in tables:
            try:
                # Count
                count_df = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine)
                count = count_df.iloc[0, 0]
                
                # Latest Time
                time_df = pd.read_sql(f"SELECT MAX(time) FROM {table}", engine)
                latest = time_df.iloc[0, 0]
                
                status = "✅ OK" if count > 0 else "❌ EMPTY"
                print(f"Table: {table.ljust(20)} | Rows: {str(count).ljust(10)} | Latest: {latest} | Status: {status}")
            except Exception as e:
                print(f"Table: {table.ljust(20)} | Error: {e}")
        
        # Verify Coinglass Funding
        print("--- Funding Rate Check ---")
        cg_count = pd.read_sql("SELECT COUNT(*) FROM derivatives_stats WHERE source = 'Coinglass_Scrape'", engine).iloc[0,0]
        print(f"Coinglass Rows: {cg_count}")
        if cg_count > 0:
             print("✅ Funding Rates are being scraped!")
        else:
             print("❌ No Coinglass Data yet.")

    except Exception as e:
        print(f"Critical DB Connection Error: {e}")

if __name__ == "__main__":
    check_db()
