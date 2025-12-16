#!/bin/bash

# Configuration
PROXY_HOST="127.0.0.1"
PROXY_PORT="8080"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MITM_SCRIPT="$SCRIPT_DIR/mitm_parser.py"
LOG_FILE="$SCRIPT_DIR/traffic_log.txt"

# Target URLs (Gems & Major Pairs)
URLS=(
    "https://www.bybit.com/trade/usdt/BTCUSDT"
    "https://www.coinglass.com/LiquidationData"
    "https://whale-alert.io/"
    "https://cryptopanic.com/"
    "https://dexcheck.ai/app/eth/whales-tracker"
    "https://www.gate.io/futures_trade/USDT/BTC_USDT"
    "https://www.deribit.com/options/BTC"
)

echo "=== Crypto Data Capture Launcher ==="

# 1. Kill existing mitmproxy instances
echo "[*] Killing existing mitmdump processes..."
pkill -f "mitmdump" || true

# 2.# Start mitmdump in the background with the parsing script
# Using python module execution to ensure we use the pip-installed version with psycopg2
echo "[*] Starting mitmproxy with logger..."
/Users/mitulpatel/.pyenv/versions/3.12.4/bin/mitmdump -s "$MITM_SCRIPT" -p $PROXY_PORT > mitm_output.log 2>&1 &
MITM_PID=$!
echo "[*] mitmproxy started (PID: $MITM_PID). Logging to $LOG_FILE"

# Wait a moment for mitmproxy to initialize
sleep 3

# 3. Launch Brave Browser with Proxy and URLs
# Note: Using the direct binary path to pass arguments correctly on macOS
BRAVE_PATH="/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

if [ -f "$BRAVE_PATH" ]; then
    echo "[*] Launching Brave Browser..."
    "$BRAVE_PATH" \
        --proxy-server="http://$PROXY_HOST:$PROXY_PORT" \
        --ignore-certificate-errors \
        "${URLS[@]}" &
else
    echo "[!] Brave Browser not found at standard path. Trying 'open' command (proxy args might not work)..."
    open -a "Brave Browser" --args --proxy-server="http://$PROXY_HOST:$PROXY_PORT" "${URLS[@]}"
fi

echo "[*] System is running."
echo "[*] Check $LOG_FILE for captured traffic."
echo "[*] Press Ctrl+C to stop the capture (will kill mitmproxy)."
echo "========================================"

# Keep script running to maintain background process
wait $MITM_PID
