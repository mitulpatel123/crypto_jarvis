import pandas as pd
from sqlalchemy import create_engine
import config

engine = create_engine(config.DB_URI)
try:
    df = pd.read_sql("SELECT * FROM market_features WHERE feature_group='orderflow' ORDER BY time DESC LIMIT 5", engine)
    print("ORDER FLOW DATA:")
    print(df)
    
    print("\nCOUNCIL LOGS (LAST 1000 bytes):")
    with open(f"{config.LOG_DIR}/council.log", "r") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(size - 2000, 0))
        print(f.read())
except Exception as e:
    print(e)
