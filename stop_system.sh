#!/bin/bash
echo "=== Stopping Crypto System ==="

if [ -f "logs/feed.pid" ]; then
    kill $(cat logs/feed.pid) 2>/dev/null
    rm logs/feed.pid
    echo "[*] Direct Feed stopped."
fi

if [ -f "logs/sniffer.pid" ]; then
    kill $(cat logs/sniffer.pid) 2>/dev/null
    rm logs/sniffer.pid
    echo "[*] Sniffer stopped."
fi

if [ -f "logs/dashboard.pid" ]; then
    kill $(cat logs/dashboard.pid) 2>/dev/null
    rm logs/dashboard.pid
    echo "[*] Dashboard stopped."
fi

echo "✅ System Halted."
