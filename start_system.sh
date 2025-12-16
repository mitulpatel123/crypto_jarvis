#!/bin/bash

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
mkdir -p "run"

echo "--- Starting Crypto Jarvis System ---"

# 1. Start Direct Feed (Binance/Bybit/Deribit)
echo "[1/5] Starting Direct Data Feed..."
nohup python3 -u direct_feed.py > "$LOG_DIR/feed.log" 2>&1 &
FEED_PID=$!
echo "      -> PID: $FEED_PID"
echo $FEED_PID > "run/feed.pid"

# 2. Start MITM Sniffer (for Browser Data)
echo "[2/5] Starting MITM Sniffer..."
nohup mitmdump -s mitm_parser.py > "$LOG_DIR/mitm.log" 2>&1 &
SNIFFER_PID=$!
echo "      -> PID: $SNIFFER_PID"
echo $SNIFFER_PID > "run/sniffer.pid"

# 3. Start Feature Engine (AI Bridge)
echo "[3/5] Starting Feature Engine (Real-Time Calc)..."
nohup python3 -u feature_engine.py > "$LOG_DIR/feature_engine.log" 2>&1 &
FE_PID=$!
echo "      -> PID: $FE_PID"
echo $FE_PID > "run/feature_engine.pid"

# 4. Start AI Council Service
if [ -f "run/council.pid" ] && kill -0 $(cat run/council.pid) 2>/dev/null; then
  echo "[4/5] Council AI is already running."
else
  echo "[4/5] Starting Council AI..."
  nohup python3 -u council.py > logs/council.log 2>&1 &
  COUNCIL_PID=$!
  echo "      -> PID: $COUNCIL_PID"
  echo $COUNCIL_PID > run/council.pid
fi

# 5. Start Browser Bot (News + Funding Scraper)
echo "[5/6] Starting Browser Bot (News/Funding)..."
nohup python3 -u browser_bot.py > "$LOG_DIR/browser_bot.log" 2>&1 &
BOT_PID=$!
echo "      -> PID: $BOT_PID"
echo $BOT_PID > "run/browser_bot.pid"

# 6. Start Dashboard
echo "[6/6] Starting Analytics Dashboard..."
nohup streamlit run dashboard.py > "$LOG_DIR/dashboard.log" 2>&1 &
DASH_PID=$!
echo "      -> PID: $DASH_PID"
echo $DASH_PID > "run/dashboard.pid"

echo "System Startup Complete. Monitor logs at logs/*.log"
