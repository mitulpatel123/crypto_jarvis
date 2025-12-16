#!/bin/bash

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

echo "=== Crypto Trading System: Unified Launcher ==="
echo "[*] Initializing Phase 6 Hybrid Architecture..."

# 1. Start Direct Feed (Market Data)
echo "[1/3] Starting Direct Feed (Binance/Bybit/Deribit)..."
nohup python3 direct_feed.py > "$LOG_DIR/feed.log" 2>&1 &
FEED_PID=$!
echo "      -> PID: $FEED_PID"
echo $FEED_PID > "$LOG_DIR/feed.pid"

# 2. Start MITM Proxy (Alternative Data - News/Whales)
echo "[2/3] Starting Sniffer (News/Whales)..."
pkill -f "mitmdump" || true
# We use the previous start logic but backgrounded properly
/Users/mitulpatel/.pyenv/versions/3.12.4/bin/mitmdump -s "mitm_parser.py" -p 8080 > "$LOG_DIR/sniffer.log" 2>&1 &
SNIFFER_PID=$!
echo "      -> PID: $SNIFFER_PID"
echo $SNIFFER_PID > "$LOG_DIR/sniffer.pid"

# 3. Start Dashboard
echo "[3/3] Starting Analytics Dashboard..."
nohup streamlit run dashboard.py > "$LOG_DIR/dashboard.log" 2>&1 &
DASH_PID=$!
echo "      -> PID: $DASH_PID"
echo $DASH_PID > "$LOG_DIR/dashboard.pid"

echo "==============================================="
echo "✅ SYSTEM LIVE"
echo "   - Dashboard: http://localhost:8501"
echo "   - Logs:      $LOG_DIR/"
echo "   - Mode:      Hybrid (Direct Feed + Clean Sniffer)"
echo "==============================================="
echo "To Stop System: ./stop_system.sh"
