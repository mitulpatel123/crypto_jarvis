#!/bin/bash
echo "[*] Stops Dashboard & Parser..."
killall -9 python3 2>/dev/null

echo "[*] Terminating DB Connections..."
docker exec timescaledb psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid();" >/dev/null 2>&1

echo "[*] Truncating Tables..."
docker exec timescaledb psql -U postgres -c "TRUNCATE market_ticks, derivatives_stats, news_sentiment;"

echo "✅ Database Wiped. Ready for fresh capture."
