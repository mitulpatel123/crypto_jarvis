import asyncio
import json
import time
import random
import aiohttp
import psycopg2
import logging
import nest_asyncio
from psycopg2 import pool
from datetime import datetime, timezone
import config  # Centralized Config

# Patch asyncio to allow nested event loops (safety net)
nest_asyncio.apply()

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/feed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DirectFeed")

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
        # Use URI from config (Port 5433 for Crypto_Jarvis isolation)
        self.pool = psycopg2.pool.SimpleConnectionPool(1, 10, config.DB_URI)
    
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
            logger.info(f"Loaded {len(self.proxies)} proxies.")
        except Exception as e:
            logger.error(f"Failed to load proxies: {e}")

    def get_random_proxy(self):
        if not self.proxies: return None
        return random.choice(self.proxies)

class MarketFeed:
    def __init__(self, db_pool, proxy_manager):
        self.db_pool = db_pool
        self.proxy_manager = proxy_manager
        self.running = True
        # Queue will be initialized in run() to match the running loop
        self.write_queue = None 

    async def db_writer(self):
        conn = self.db_pool.get_conn()
        cursor = conn.cursor()
        logger.info("DB Writer Started")
        
        batch = []
        last_flush = time.time()
        
        while self.running:
            try:
                try:
                    # Non-blocking get with timeout
                    data = await asyncio.wait_for(self.write_queue.get(), timeout=1.0)
                    batch.append(data)
                except asyncio.TimeoutError:
                    pass
                
                now = time.time()
                # Configurable Batch Size
                if len(batch) >= config.BATCH_SIZE or (now - last_flush > 1.0 and batch):
                    self.flush_batch(cursor, batch)
                    conn.commit()
                    batch = []
                    last_flush = now
            except Exception as e:
                logger.error(f"Writer Error: {e}", exc_info=True)
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
                     datetime.fromtimestamp(t.get('timestamp', time.time()), timezone.utc), 
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
                # Use strict defaults for critical fields
                deriv_args.append((
                     datetime.fromtimestamp(t.get('timestamp', time.time()), timezone.utc), 
                     t.get('symbol'), 
                     t.get('funding_rate', 0.0), # Safer generic get
                     t.get('open_interest', 0), 
                     t.get('turnover', 0.0),
                     t.get('iv', 0), t.get('delta', 0), t.get('gamma', 0),
                     t.get('source'),
                     t.get('expiry'), t.get('strike', 0), t.get('option_type')
                ))
            args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in deriv_args)
            cursor.execute("INSERT INTO derivatives_stats (time, symbol, funding_rate, open_interest, turnover, iv, delta, gamma, source, expiry, strike, option_type) VALUES " + args_str + " ON CONFLICT DO NOTHING")

    async def queue_put(self, item):
        """Helper to handle backpressure"""
        if self.write_queue is None: return
        try:
            self.write_queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.warning("Queue Full! Dropping market data tick.")

    async def connect_binance(self):
        while self.running:
            url = f"wss://fstream.binance.com/stream?streams={'/'.join(STREAMS['binance'])}"
            proxy = self.proxy_manager.get_random_proxy()
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        logger.info("Connected to Binance Futures")
                        async for msg in ws:
                            if not self.running: break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.parse_binance(json.loads(msg.data))
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
            except Exception as e:
                logger.error(f"Binance Futures Error: {e}")
                await asyncio.sleep(5)

    async def connect_binance_spot(self):
        streams = STREAMS["binance"] # Reuse stream list since logic is identical for spot/fut symbol format in stream api
        while self.running:
            url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
            proxy = self.proxy_manager.get_random_proxy()
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        logger.info("Connected to Binance SPOT")
                        async for msg in ws:
                            if not self.running: break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.parse_binance(json.loads(msg.data), source_suffix="_Spot")
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
            except Exception as e:
                await asyncio.sleep(5)

    async def parse_binance(self, data, source_suffix=""):
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
            await self.queue_put(entry)
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
                await self.queue_put(entry)

    async def connect_bybit(self):
        url_linear = "wss://stream.bybit.com/v5/public/linear"
        url_option = "wss://stream.bybit.com/v5/public/option"
        
        async def run_ws(url, name, sub_args):
            while self.running:
                proxy = self.proxy_manager.get_random_proxy()
                try:
                    async with aiohttp.ClientSession() as session:
                        logger.info(f"Connecting to Bybit {name}...")
                        async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                            logger.info(f"Connected to Bybit {name}")
                            await ws.send_json({"op": "subscribe", "args": sub_args})
                            async for msg in ws:
                                if not self.running: break
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self.parse_bybit(json.loads(msg.data))
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    break
                except Exception as e:
                    logger.error(f"Bybit {name} Error: {e}")
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

    async def parse_bybit(self, data):
        topic = data.get("topic", "")
        if "publicTrade" in topic:
            for item in data.get("data", []):
                await self.queue_put({
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
                 await self.queue_put({
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
                except: pass

                await self.queue_put({
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
                    "expiry": None, 
                    "strike": strike,
                    "option_type": option_type
                })

    async def connect_deribit(self):
        url = "wss://www.deribit.com/ws/api/v2"
        while self.running:
            proxy = self.proxy_manager.get_random_proxy()
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy, ssl=False) as ws:
                        logger.info("Connected to Deribit")
                        
                        all_channels = []
                        all_channels.extend(["ticker.BTC-PERPETUAL.100ms", "ticker.ETH-PERPETUAL.100ms", "ticker.SOL-PERPETUAL.100ms"])
                        
                        for currency in ["BTC", "ETH", "SOL"]:
                            msg = {
                                "jsonrpc": "2.0", "id": 8888, "method": "public/get_instruments",
                                "params": {"currency": currency, "kind": "option", "expired": False}
                            }
                            await ws.send_json(msg)
                            response = await ws.receive_json()
                            instruments = response.get("result", [])
                            for inst in instruments:
                                all_channels.append(f"ticker.{inst['instrument_name']}.100ms")
                        
                        logger.info(f"Found {len(all_channels)} instruments. Subscribing in batches...")

                        batch_size = 100
                        for i in range(0, len(all_channels), batch_size):
                            batch = all_channels[i:i + batch_size]
                            sub_msg = { "jsonrpc": "2.0", "id": 1000 + i, "method": "public/subscribe", "params": {"channels": batch} }
                            await ws.send_json(sub_msg)
                            await asyncio.sleep(0.1)

                        async for msg_raw in ws:
                            if not self.running: break
                            if msg_raw.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg_raw.data)
                                if "params" in data: 
                                    await self.parse_deribit(data["params"])
                                    
            except Exception as e:
                logger.error(f"Deribit Error: {e}")
                await asyncio.sleep(5)

    async def parse_deribit(self, data):
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
                await self.queue_put({
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
                    "expiry": expiry, "strike": strike, "option_type": option_type,
                    "turnover": 0.0, 
                    "funding_rate": float(item.get("funding_8h", item.get("current_funding", 0))) # Prioritize 8h rate
                })

    async def run(self):
        # Initialize Queue inside the Async Loop (CRITICAL FIX)
        self.write_queue = asyncio.Queue(maxsize=config.QUEUE_MAX_SIZE)

        tasks = [
            asyncio.create_task(self.db_writer()),
            asyncio.create_task(self.connect_binance()), 
            asyncio.create_task(self.connect_binance_spot()), 
            asyncio.create_task(self.connect_bybit()),
            asyncio.create_task(self.connect_deribit()) 
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Stopping...")

if __name__ == "__main__":
    db_pool = DatabasePool()
    proxy_mgr = ProxyManager(config.PROXY_FILE_PATH)
    feed = MarketFeed(db_pool, proxy_mgr)
    
    try:
        asyncio.run(feed.run())
    except KeyboardInterrupt:
        pass
    finally:
        db_pool.close()
