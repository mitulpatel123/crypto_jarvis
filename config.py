import os
import sys

# --- Project Isolation Settings ---
# Unique ports/paths to prevent conflicts with Indian/Forex projects
PROJECT_NAME = "Crypto_Jarvis"
DB_PORT = 5433
STREAMLIT_PORT = 8501

# --- Database ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Constructed URI
# Note: Keeping the explicit port 5433 is critical for isolation.
# Do not change this to 5432 unless you intentionally want to break isolation.
DB_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Proxy ---
PROXY_FILE_PATH = os.getenv("PROXY_FILE", "../iproyal-proxies.txt")

# --- Logging ---
# Paths
LOG_DIR = "logs"
LOG_LEVEL = "INFO" 

# --- Tuning ---
# Max size of queues before dropping old data (Backpressure)
QUEUE_MAX_SIZE = 10000 
BATCH_SIZE = 100
BATCH_INTERVAL = 1.0 # seconds
