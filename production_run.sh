#!/bin/bash

# --- Configuration ---
# export PATH="/Users/mitulpatel/Library/Python/3.9/bin:$PATH"  <-- CAUSING VERSION CONFLICT
LOG_DIR="logs"
RUN_DIR="run"
mkdir -p "$LOG_DIR"
mkdir -p "$RUN_DIR"

echo "=============================================="
echo "   CRYPTO JARVIS - PRODUCTION LAUNCHER"
echo "=============================================="

# 0. Pre-Flight Cleanup (Kill Stale Processes)
echo "[!] Cleaning up old processes..."
pkill -f "mitmdump"
pkill -f "chrome"
pkill -f "chromedriver"
pkill -f "direct_feed.py"
pkill -f "council.py"
pkill -f "dashboard.py"
pkill -f "feature_engine.py"
# Force kill port 8080 users
lsof -ti:8080 | xargs kill -9 2>/dev/null
sleep 2

# 0.2 Fresh Start for Mitmproxy (Fixes upgrade issues)
rm -rf ~/.mitmproxy_backup
mv ~/.mitmproxy ~/.mitmproxy_backup 2>/dev/null
mkdir -p ~/.mitmproxy

# 0.3 Dependency Check & Install
echo "[*] Checking Dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt --upgrade > "$LOG_DIR/install.log" 2>&1
if [ $? -eq 0 ]; then
    echo "    -> Dependencies OK."
else
    echo "    -> Warning: Dependency install issue. Check logs/install.log"
fi

# 1. Cleanup Function
cleanup() {
    echo ""
    echo "[!] Shutting down all services..."
    pkill -P $$ 
    # Kill specific PIDs if they exist
    [ -f "$RUN_DIR/mitm.pid" ] && kill $(cat "$RUN_DIR/mitm.pid") 2>/dev/null
    [ -f "$RUN_DIR/feed.pid" ] && kill $(cat "$RUN_DIR/feed.pid") 2>/dev/null
    [ -f "$RUN_DIR/council.pid" ] && kill $(cat "$RUN_DIR/council.pid") 2>/dev/null
    [ -f "$RUN_DIR/browser.pid" ] && kill -9 $(cat "$RUN_DIR/browser.pid") 2>/dev/null
    [ -f "$RUN_DIR/dashboard.pid" ] && kill $(cat "$RUN_DIR/dashboard.pid") 2>/dev/null
    [ -f "$RUN_DIR/feature.pid" ] && kill $(cat "$RUN_DIR/feature.pid") 2>/dev/null
    
    # Force auto-kill port 8080 hogs to prevent "Address in use"
    fuser -k 8080/tcp >/dev/null 2>&1 || lsof -ti:8080 | xargs kill -9 >/dev/null 2>&1
    
    # Clean mitmdump & chrome
    pkill -f "mitmdump"
    pkill -f "chrome" 
    pkill -f "chromedriver"
    
    echo "[*] System Shutdown Complete."
    exit
}

# Trap Ctrl+C
trap cleanup SIGINT SIGTERM

# Force cleanup port 8080 before starting
lsof -ti:8080 | xargs kill -9 >/dev/null 2>&1

# 2. Start MITM Proxy (Data Ingestion Layer 1)
echo "[1/6] Starting MITM Proxy..."
# -q to silence the disconnectspam
nohup mitmdump -q -s mitm_parser.py -p 8080 > "$LOG_DIR/mitm.log" 2>&1 &
MITM_PID=$!
echo $MITM_PID > "$RUN_DIR/mitm.pid"
echo "      -> PID: $MITM_PID"
sleep 5 # Wait for proxy to bind - Increased for stability

# 3. Start Direct Feeds (Data Ingestion Layer 2)
echo "[2/6] Starting Direct Data Feeds..."
nohup python3 -u direct_feed.py > "$LOG_DIR/feed.log" 2>&1 &
FEED_PID=$!
echo $FEED_PID > "$RUN_DIR/feed.pid"
echo "      -> PID: $FEED_PID"

# 4. Start Feature Engine (Processing Layer)
echo "[3/6] Starting Feature Engine..."
nohup python3 -u feature_engine.py > "$LOG_DIR/feature_engine.log" 2>&1 &
FE_PID=$!
echo $FE_PID > "$RUN_DIR/feature.pid"
echo "      -> PID: $FE_PID"

# 5. Start Browser Bot (News Scraper)
echo "[4/6] Starting Browser Bot (News/Sentiment)..."
nohup python3 -u browser_bot.py > "$LOG_DIR/news_feed.log" 2>&1 &
BOT_PID=$!
echo $BOT_PID > "$RUN_DIR/browser.pid"
echo "      -> PID: $BOT_PID"

# 6. Start AI Council (Logic Layer)
echo "[5/6] Starting AI Council (The Brain)..."
nohup python3 -u council.py > "$LOG_DIR/council.log" 2>&1 &
COUNCIL_PID=$!
echo $COUNCIL_PID > "$RUN_DIR/council.pid"
echo "      -> PID: $COUNCIL_PID"

# 7. Start Dashboard (UI Layer)
echo "[6/6] Starting Dashboard..."
# Use python -m streamlit to ensure path finding
nohup python3 -m streamlit run dashboard.py > "$LOG_DIR/dashboard.log" 2>&1 &
DASH_PID=$!
echo $DASH_PID > "$RUN_DIR/dashboard.pid"
echo "      -> PID: $DASH_PID"

echo "=============================================="
echo "   SYSTEM IS LIVE"
echo "   Logs: tail -f logs/*.log"
echo "   UI:   http://localhost:8501"
echo "=============================================="

# Keep script alive
wait
