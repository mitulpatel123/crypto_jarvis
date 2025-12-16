import asyncio
import json
import time
import random
import aiohttp
import psycopg2
from psycopg2 import pool
from datetime import datetime

# --- Configuration ---
DB_URI = "postgres://postgres:password@127.0.0.1:5433/postgres"
PROXY_FILE = "../iproyal-proxies.txt"

# Target Data Streams
STREAMS = {
    "binance": [
        "btcusdt@aggTrade", "btcusdt@depth5@100ms", 
        "ethusdt@aggTrade", "ethusdt@depth5@100ms",
        "solusdt@aggTrade", "solusdt@depth5@100ms"
    ]
}

class DatabasePool:
    def __init__(self):
        self.pool = psycopg2.pool.SimpleConnectionPool(1, 10, DB_URI)
    
    def get_conn(self):
        return self.pool.getconn()
    
    def put_conn(self, conn):
        self.pool.putconn(conn)
        
    def close(self):
        self.pool.closeall()

class ProxyManager:
    def __init__(self, filepath):
        self.proxies = []
        self.load_proxies(filepath)
    
    def load_proxies(self, filepath):
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        parts = line.strip().split(':')
                        if len(parts) == 4:
                            # aiohttp proxy format: http://user:pass@host:port
                            self.proxies.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")
            print(f"[*] Loaded {len(self.proxies)} proxies.")
        except Exception as e:
            print(f"[!] Failed to load proxies: {e}")

    def get_random_proxy(self):
        if not self.proxies: return None
        return random.choice(self.proxies)

class MarketFeed:
    def __init__(self, db_pool, proxy_manager):
        self.db_pool = db_pool
        self.proxy_manager = proxy_manager
        self.running = True
        self.write_queue = asyncio.Queue()

    async def db_writer(self):
        conn = self.db_pool.get_conn()
        cursor = conn.cursor()
        print("[*] DB Writer Started")
        
        batch = []
        last_flush = time.time()
        
        while self.running:
            try:
                try:
                    data = await asyncio.wait_for(self.write_queue.get(), timeout=1.0)
                    batch.append(data)
                except asyncio.TimeoutError:
                    pass
                
                now = time.time()
                if len(batch) >= 100 or (now - last_flush > 1.0 and batch):
                    self.flush_batch(cursor, batch)
                    conn.commit()
                    batch = []
                    last_flush = now
            except Exception as e:
                print(f"[!] Writer Error: {e}")
                conn.rollback()
        
        self.db_pool.put_conn(conn)

    def flush_batch(self, cursor, batch):
        ticks = []
        derivs = []
        
        for item in batch:
            if item.get("source") == "Bybit_Option" or "iv" in item or item.get("source") == "Deribit":
                derivs.append(item)
            else:
                ticks.append(item)

        if ticks:
            tick_args = []
            for t in ticks:
                tick_args.append((
                     datetime.fromtimestamp(t.get('timestamp', time.time())), 
                     t.get('symbol'), 
                     t.get('price'), 
                     t.get('bid'), t.get('ask'), t.get('volume'), 
                     t.get('source'), t.get('side')
                ))
            args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in tick_args)
            cursor.execute("INSERT INTO market_ticks (time, symbol, price, bid, ask, volume, source, side) VALUES " + args_str + " ON CONFLICT DO NOTHING")

        if derivs:
            deriv_args = []
            for t in derivs:
                deriv_args.append((
                     datetime.fromtimestamp(t.get('timestamp', time.time())), 
                     t.get('symbol'), 
                     0.0,
                     t.get('open_interest', 0), 
                     0.0,
                     t.get('iv', 0), t.get('delta', 0), t.get('gamma', 0),
                     t.get('source'),
                     t.get('expiry'), t.get('strike', 0), t.get('option_type')
                ))
            args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in deriv_args)
            cursor.execute("INSERT INTO derivatives_stats (time, symbol, funding_rate, open_interest, turnover, iv, delta, gamma, source, expiry, strike, option_type) VALUES " + args_str + " ON CONFLICT DO NOTHING")

    async def connect_binance(self):
        while self.running:
            url = f"wss://fstream.binance.com/stream?streams={'/'.join(STREAMS['binance'])}"
            proxy = self.proxy_manager.get_random_proxy()
            print(f"[*] Connecting to Binance (Proxy)...")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        print("[*] Connected to Binance")
                        async for msg in ws:
                            if not self.running: break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.parse_binance(json.loads(msg.data))
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
            except Exception as e:
                print(f"[!] Binance Error: {e}")
                await asyncio.sleep(5)

    async def connect_binance_spot(self):
        # Spot URL: stream.binance.com:9443
        # Streams: <symbol>@aggTrade / <symbol>@depth5
        streams = [
            "btcusdt@aggTrade", "btcusdt@depth5@100ms",
            "ethusdt@aggTrade", "ethusdt@depth5@100ms",
            "solusdt@aggTrade", "solusdt@depth5@100ms"
        ]
        
        while self.running:
            url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
            proxy = self.proxy_manager.get_random_proxy()
            # print(f"[*] Connecting to Binance SPOT (Proxy)...")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        print("[*] Connected to Binance SPOT")
                        async for msg in ws:
                            if not self.running: break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                # Reuse parsing logic (payload structure is same for Spot/Futures aggTrade)
                                self.parse_binance(json.loads(msg.data), source_suffix="_Spot")
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
            except Exception as e:
                # print(f"[!] Binance Spot Error: {e}") # Reduce noise
                await asyncio.sleep(5)

    def parse_binance(self, data, source_suffix=""):
        # source_suffix allows distinguishing Futures vs Spot in DB source column
        stream = data.get("stream", "")
        payload = data.get("data", {})
        if "aggTrade" in stream:
            is_maker = payload.get("m")
            entry = {
                "timestamp": float(payload.get("T")) / 1000,
                "symbol": payload.get("s"),
                "price": float(payload.get("p")),
                "volume": float(payload.get("q")),
                "bid": None, "ask": None,
                "side": "SELL" if is_maker else "BUY",
                "source": f"Binance_AggTrade{source_suffix}"
            }
            self.write_queue.put_nowait(entry)
        elif "depth" in stream:
            bids = payload.get("b", [])
            asks = payload.get("a", [])
            if bids and asks:
                entry = {
                    "timestamp": float(payload.get("T")) / 1000,
                    "symbol": payload.get("s"),
                    "price": (float(bids[0][0]) + float(asks[0][0])) / 2,
                    "bid": float(bids[0][0]), "ask": float(asks[0][0]),
                    "volume": 0, "side": None, 
                    "source": f"Binance_Depth{source_suffix}"
                }
                self.write_queue.put_nowait(entry)

    async def connect_bybit(self):
        url_linear = "wss://stream.bybit.com/v5/public/linear"
        url_option = "wss://stream.bybit.com/v5/public/option"
        
        async def run_ws(url, name, sub_args):
            while self.running:
                proxy = self.proxy_manager.get_random_proxy()
                try:
                    async with aiohttp.ClientSession() as session:
                        print(f"[*] Connecting to Bybit {name} (Proxy)...")
                        async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                            print(f"[*] Connected to Bybit {name}")
                            await ws.send_json({"op": "subscribe", "args": sub_args})
                            async for msg in ws:
                                if not self.running: break
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    self.parse_bybit(json.loads(msg.data))
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    break
                except Exception as e:
                    print(f"[!] Bybit {name} Error: {e}")
                    await asyncio.sleep(5)

        args_linear = [
            "publicTrade.BTCUSDT", "publicTrade.ETHUSDT", "publicTrade.SOLUSDT",
            "orderbook.1.BTCUSDT", "orderbook.1.ETHUSDT", "orderbook.1.SOLUSDT"
        ]
        args_option = ["tickers.BTC", "tickers.ETH", "tickers.SOL"]
        
        await asyncio.gather(
            run_ws(url_linear, "Linear", args_linear),
            run_ws(url_option, "Option", args_option)
        )

    def parse_bybit(self, data):
        topic = data.get("topic", "")
        if "publicTrade" in topic:
            for item in data.get("data", []):
                self.write_queue.put_nowait({
                    "timestamp": float(item.get("T")) / 1000,
                    "symbol": item.get("s"),
                    "price": float(item.get("p")),
                    "volume": float(item.get("v")),
                    "bid": None, "ask": None,
                    "side": item.get("S").upper(), 
                    "source": "Bybit_Trade"
                })
        elif "orderbook" in topic:
            item = data.get("data", {})
            bids = item.get("b", [])
            asks = item.get("a", [])
            if bids and asks:
                 self.write_queue.put_nowait({
                    "timestamp": float(data.get("ts")) / 1000,
                    "symbol": item.get("s"),
                    "price": (float(bids[0][0]) + float(asks[0][0])) / 2,
                    "bid": float(bids[0][0]), "ask": float(asks[0][0]),
                    "volume": 0, "side": None, "source": "Bybit_Book"
                })
        elif "tickers" in topic:
            for item in data.get("data", []):
                symbol = item.get("symbol")
                expiry, strike, option_type = None, None, None
                try:
                    parts = symbol.split('-')
                    if len(parts) == 4:
                        strike = float(parts[2])
                        option_type = "CALL" if parts[3] == "C" else "PUT"
                        # Date parsing omitted for brevity, using string or None
                except: pass

                self.write_queue.put_nowait({
                    "timestamp": time.time(),
                    "symbol": symbol,
                    "price": float(item.get("lastPrice")) if item.get("lastPrice") else 0,
                    "bid": float(item.get("bid1Price")) if item.get("bid1Price") else 0,
                    "ask": float(item.get("ask1Price")) if item.get("ask1Price") else 0,
                    "volume": float(item.get("volume24h")) if item.get("volume24h") else 0,
                    "side": None, 
                    "source": "Bybit_Option",
                    "open_interest": float(item.get("open_interest", 0)),
                    "iv": float(item.get("markIv", 0)),
                    "delta": float(item.get("delta", 0)),
                    "gamma": float(item.get("gamma", 0)),
                    "expiry": None, # Parsing requires logic, keeping simple for now
                    "strike": strike,
                    "option_type": option_type
                })

    async def connect_deribit(self):
        url = "wss://www.deribit.com/ws/api/v2"
        while self.running:
            proxy = self.proxy_manager.get_random_proxy()
            print(f"[*] Connecting to Deribit (Proxy)...")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        print("[*] Connected to Deribit")
                        
                        # STEP 1: Ask Deribit for the list of ALL active options
                        # We do this for BTC, ETH, and SOL
                        all_channels = []
                        # Add Perps manually first
                        all_channels.extend(["ticker.BTC-PERPETUAL.100ms", "ticker.ETH-PERPETUAL.100ms", "ticker.SOL-PERPETUAL.100ms"])
                        
                        for currency in ["BTC", "ETH", "SOL"]:
                            msg = {
                                "jsonrpc": "2.0",
                                "id": 8888,
                                "method": "public/get_instruments",
                                "params": {
                                    "currency": currency,
                                    "kind": "option",
                                    "expired": False
                                }
                            }
                            await ws.send_json(msg)
                            response = await ws.receive_json()
                            
                            # Extract every single symbol name
                            instruments = response.get("result", [])
                            for inst in instruments:
                                symbol = inst["instrument_name"]
                                # Add to our subscription list
                                all_channels.append(f"ticker.{symbol}.100ms")
                        
                        print(f"[*] Found {len(all_channels)} instruments. Subscribing in batches...")

                        # STEP 2: Subscribe to them in batches
                        # (Deribit might reject if you send 5000 in one message, so we split it)
                        batch_size = 100
                        for i in range(0, len(all_channels), batch_size):
                            batch = all_channels[i:i + batch_size]
                            sub_msg = {
                                "jsonrpc": "2.0",
                                "id": 1000 + i,
                                "method": "public/subscribe",
                                "params": {"channels": batch}
                            }
                            await ws.send_json(sub_msg)
                            # Sleep briefly to avoid hitting rate limits
                            await asyncio.sleep(0.1)

                        print("[*] Successfully subscribed to ALL options.")

                        # STEP 3: Listen for the data
                        async for msg_raw in ws:
                            if not self.running: break
                            if msg_raw.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg_raw.data)
                                if "params" in data: 
                                    self.parse_deribit(data["params"])
                                    
            except Exception as e:
                print(f"[!] Deribit Error: {e}")
                await asyncio.sleep(5)

    def parse_deribit(self, data):
        channel = data.get("channel", "")
        item = data.get("data", {})
        if "ticker" in channel:
            symbol = item.get("instrument_name")
            price = item.get("last_price")
            greeks = item.get("greeks", {})
            iv = item.get("mark_iv")
            expiry, strike, option_type = None, None, None
            import re
            try:
                 match = re.search(r'^[A-Z]+-(\d{1,2}[A-Z]{3}\d{2})-(\d+)-([CP])$', symbol)
                 if match:
                     date_str = match.group(1)
                     strike = float(match.group(2))
                     option_type = "CALL" if match.group(3) == "C" else "PUT"
                     expiry = datetime.strptime(date_str, "%d%b%y").replace(tzinfo=None)
            except: pass

            if symbol and price:
                self.write_queue.put_nowait({
                    "timestamp": float(item.get("timestamp")) / 1000,
                    "symbol": symbol,
                    "price": float(price),
                    "bid": float(item.get("best_bid_price", 0)),
                    "ask": float(item.get("best_ask_price", 0)),
                    "volume": float(item.get("stats", {}).get("volume", 0)),
                    "side": None, 
                    "source": "Deribit",
                    "open_interest": float(item.get("open_interest", 0)),
                    "iv": float(iv) if iv else None,
                    "delta": float(greeks.get("delta")) if greeks else None,
                    "gamma": float(greeks.get("gamma")) if greeks else None,
                    "expiry": expiry, "strike": strike, "option_type": option_type
                })

    async def run(self):
        tasks = [
            asyncio.create_task(self.db_writer()),
            asyncio.create_task(self.connect_binance()), # Futures
            asyncio.create_task(self.connect_binance_spot()), # Spot
            asyncio.create_task(self.connect_bybit()),
            asyncio.create_task(self.connect_deribit()) 
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("[*] Stopping...")

if __name__ == "__main__":
    db_pool = DatabasePool()
    proxy_mgr = ProxyManager(PROXY_FILE)
    feed = MarketFeed(db_pool, proxy_mgr)
    
    try:
        asyncio.run(feed.run())
    except KeyboardInterrupt:
        pass
    finally:
        db_pool.close()
