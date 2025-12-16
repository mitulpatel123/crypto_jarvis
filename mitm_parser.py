import mitmproxy.http
from mitmproxy import ctx
import re
from datetime import datetime
import json
import os
import zlib
import time
import threading
import queue
import traceback
import psycopg2 

# Target domains
TARGET_DOMAINS = [
    "bybit.com",
    "coinglass.com",
    "deribit.com",
    "whale-alert.io",
    "cryptopanic.com",
    "dextools.io",
    "dexcheck.ai",
    "gate.io",
    "coinapi.io",
    "binance.com"
]

OUTPUT_FILE = "captured_data.jsonl"

try:
    import brotli
except ImportError:
    brotli = None

class DatabaseWriter(threading.Thread):
    def __init__(self, q, db_uri=None):
        super().__init__()
        self.queue = q
        self.db_uri = db_uri or "postgres://postgres:password@127.0.0.1:5433/postgres"
        self.batch_size = 100
        self.flush_interval = 1.0 # seconds
        self.daemon = True # Auto-kill on exit
        self.running = True

    def run(self):
        debug_path = os.path.join(os.path.dirname(__file__), "writer_debug.log")
        with open(debug_path, "w") as f:
             f.write(f"[Writer] Process started. PID: {os.getpid()}\n")
        
        buffer = []
        last_flush = time.time()
        
        # Real connection
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(self.db_uri)
            cursor = conn.cursor()
            with open(debug_path, "a") as f: f.write(f"[Writer] Connected to DB: {self.db_uri}\n")
        except Exception as e:
             with open(debug_path, "a") as f: f.write(f"[Writer] DB CONNECT FAIL: {e}\n")

        while self.running:
            try:
                # Non-blocking fetch with small timeout
                try:
                    record = self.queue.get(timeout=0.1)
                    buffer.append(record)
                except queue.Empty:
                    pass

                # Auto-flush on time or batch size
                now = time.time()
                if len(buffer) >= self.batch_size or (now - last_flush > self.flush_interval and buffer):
                    if conn:
                        self.flush_batch(cursor, buffer)
                        conn.commit()
                    buffer = []
                    last_flush = now
                
            except Exception as e:
                with open(debug_path, "a") as f: f.write(f"Error: {e}\n")
                if conn:
                    try: conn.rollback()
                    except: pass
                traceback.print_exc()
        
        if conn: conn.close()
    
    def flush_batch(self, cursor, batch):
        # Separate by table
        ticks = [r for r in batch if r['type'] == 'tick']
        derivs = [r for r in batch if r['type'] == 'derivative']
        news = [r for r in batch if r['type'] == 'news']
        
        if ticks:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t['timestamp'])), 
                 t['symbol'], t['price'], t['bid'], t['ask'], t['volume'], t['source'],
                 t.get('side')
             )).decode('utf-8') for t in ticks)
             cursor.execute("INSERT INTO market_ticks (time, symbol, price, bid, ask, volume, source, side) VALUES " + args_str + " ON CONFLICT DO NOTHING")

        # Bulk Insert Derivatives
        if derivs:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(d['timestamp'])), 
                 d['symbol'], d['funding_rate'], d['open_interest'], d['turnover'], 
                 d['iv'], d['delta'], d['gamma'],
                 d.get('expiry'), d.get('strike'), d.get('option_type')
             )).decode('utf-8') for d in derivs)
             cursor.execute("INSERT INTO derivatives_stats (time, symbol, funding_rate, open_interest, turnover, iv, delta, gamma, expiry, strike, option_type) VALUES " + args_str + " ON CONFLICT DO NOTHING")

        # Bulk Insert News
        if news:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(n['timestamp'])), 
                 n['source'], n['title'], n['currency'], n['sentiment'], n['amount']
             )).decode('utf-8') for n in news)
             cursor.execute("INSERT INTO news_sentiment (time, source, title, currency, sentiment, amount) VALUES " + args_str + " ON CONFLICT DO NOTHING")

    def stop(self):
        self.running = False


class CryptoParser:
    def __init__(self):
        self.output_path = os.path.join(os.path.dirname(__file__), OUTPUT_FILE)
        with open(self.output_path, "w") as f:
            pass
        self.debug_log = os.path.join(os.path.dirname(__file__), "parser_debug.log")
        with open(self.debug_log, "w") as f:
            f.write("Parser initialized\n")
            
        # Initialize DB Writer Pipeline
        self.queue = queue.Queue()
        self.writer = DatabaseWriter(self.queue)
        self.writer.start()
        with open(self.debug_log, "a") as f:
            f.write("DB Writer Process Started\n")

    def __del__(self):
        if hasattr(self, 'writer'):
            self.writer.stop()

    def is_target(self, url):
        return any(domain in url for domain in TARGET_DOMAINS)

    def decode_message(self, content):
        """ robust decode/decompress """
        if not isinstance(content, bytes):
            return str(content)

        # 1. Try generic UTF-8 decode first (fastest, most common)
        try:
            return content.decode('utf-8')
        except:
            pass
        
        # 2. Try GZIP (most common compression for websockets)
        try:
            # 16 + MAX_WBITS handles gzip headers
            return zlib.decompress(content, 16 + zlib.MAX_WBITS).decode('utf-8')
        except:
            pass

        # 3. Try Raw Deflate (zlib)
        try:
            return zlib.decompress(content, -zlib.MAX_WBITS).decode('utf-8')
        except:
            pass

        # 4. Try Brotli
        if brotli:
            try:
                return brotli.decompress(content).decode('utf-8')
            except:
                pass
        
        return None

    def push_to_queue(self, record_type, data):
        """Push structured data to DB Writer Queue"""
        payload = data.copy()
        payload['type'] = record_type
        # payload['timestamp'] is already in data usually
        self.queue.put(payload)

    def save_csv(self, row):
        # Legacy hook: Redirect to Queue
        # Determine type based on fields
        rtype = 'tick'
        if row.get('funding_rate') or row.get('iv'):
            rtype = 'derivative'
        
        self.push_to_queue(rtype, row)

    def save_news_csv(self, row):
        # Legacy hook: Redirect to Queue
        self.push_to_queue('news', row)

    def parse_bybit(self, data):
        # Bybit Topics observed: mark_price.30..MBTCUSDT, orderBook_20@m1.H.BTCUSDT, recently_trade.H.BTCUSDT
        topic = data.get("topic", "")
        
        # Capture Price/Ticker/Trade data
        if any(x in topic for x in ["ticker", "trade", "mark_price", "book"]):
            payload = data.get("data", [])
            if isinstance(payload, dict): payload = [payload]
            
            for item in payload:
                # Map Bybit V5 fields
                symbol = item.get("s", "Unknown")
                price = item.get("c") or item.get("p") or item.get("p1")
                bid = item.get("b1")
                ask = item.get("a1")
                volume = item.get("v") or item.get("q")
                
                # Side Inference
                side = None
                if "m" in item:
                    side = "SELL" if item.get("m") else "BUY"
                elif item.get("S"): # Orderbook snapshot side
                    side = item.get("S")

                # If we found at least a price, save it
                if price:
                    self.save_csv({
                        "timestamp": time.time(),
                        "source": f"Bybit_{topic.split('.')[0]}",
                        "symbol": symbol,
                        "price": float(price) if price else None,
                        "bid": float(bid) if bid else None,
                        "ask": float(ask) if ask else None,
                        "volume": float(volume) if volume else None,
                        "side": side,
                        "open_interest": float(item.get("openInterest")) if item.get("openInterest") else None,
                        "funding_rate": float(item.get("fundingRate")) if item.get("fundingRate") else None,
                        "turnover": float(item.get("turnover24h")) if item.get("turnover24h") else None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })

    def parse_binance(self, data):
        try:
            stream = data.get("stream", "")
            payload = data.get("data", {})
            
            # Case 1: Liquidations (forceOrder)
            if "forceOrder" in stream:
                o = payload.get("o", {})
                symbol = o.get("s")
                price = o.get("p")
                side = o.get("S")
                qty = o.get("q")
                if symbol and price:
                     self.save_csv({
                        "timestamp": time.time(),
                        "source": "Binance_Liq",
                        "symbol": symbol,
                        "price": float(price),
                        "bid": None, "ask": None,
                        "volume": float(qty),
                        "side": side,
                        "open_interest": None,
                        "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })

            # Case 2: Aggregated Trades (aggTrade) - High Volume!
            elif "aggTrade" in stream:
                symbol = payload.get("s")
                price = payload.get("p")
                qty = payload.get("q")
                is_maker = payload.get("m") # True = Sell (Taker sold into bid)
                
                if symbol and price:
                    self.save_csv({
                        "timestamp": float(payload.get("T", time.time()*1000)) / 1000,
                        "source": "Binance_Spot",
                        "symbol": symbol,
                        "price": float(price),
                        "bid": None, "ask": None,
                        "volume": float(qty),
                        "side": "SELL" if is_maker else "BUY",
                        "open_interest": None,
                        "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })

            # Case 3: Mini Ticker (24hrTicker)
            elif "ticker" in stream:
                symbol = payload.get("s")
                close_price = payload.get("c")
                volume = payload.get("v")
                if symbol and close_price:
                     self.save_csv({
                        "timestamp": time.time(),
                        "source": "Binance_Ticker",
                        "symbol": symbol,
                        "price": float(close_price),
                        "bid": None, "ask": None,
                        "volume": float(volume),
                        "side": None,
                        "open_interest": None,
                        "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })
        except Exception:
            pass # Fail silently on bad frames to prevent crash

    def parse_deribit(self, data):
        try:
            params = data.get("params", {})
            channel = params.get("channel", "")
            if "ticker" in channel:
                item = params.get("data", {})
                symbol = item.get("instrument_name")
                price = item.get("last_price")
                greeks = item.get("greeks", {})
                iv = item.get("mark_iv")
                
                # Phase 4.5: Option Parsing (Robust)
                expiry, strike, option_type = None, None, None
                try:
                    # Example: BTC-29DEC23-50000-C
                    match = re.search(r'^[A-Z]+-(\d{1,2}[A-Z]{3}\d{2})-(\d+)-([CP])$', symbol)
                    if match:
                        date_str = match.group(1)
                        expiry = datetime.strptime(date_str, "%d%b%y").replace(tzinfo=None)
                        strike = float(match.group(2))
                        option_type = "CALL" if match.group(3) == "C" else "PUT"
                except: 
                    pass # Regex failed, consume gracefully

                if symbol and price:
                    self.save_csv({
                        "timestamp": time.time(),
                        "source": "Deribit",
                        "symbol": symbol,
                        "price": float(price) if price else None,
                        "bid": float(item.get("best_bid_price")) if item.get("best_bid_price") else None,
                        "ask": float(item.get("best_ask_price")) if item.get("best_ask_price") else None,
                        "volume": float(item.get("stats", {}).get("volume")) if item.get("stats", {}).get("volume") else None,
                        "side": item.get("tick_direction"), # 0=Plus, 1=Minus...
                        "open_interest": float(item.get("open_interest")) if item.get("open_interest") else None,
                        "funding_rate": None, "turnover": None,
                        "iv": float(iv) if iv else None,
                        "delta": float(greeks.get("delta")) if greeks and greeks.get("delta") else None,
                        "gamma": float(greeks.get("gamma")) if greeks and greeks.get("gamma") else None,
                        "expiry": str(expiry) if expiry else None,
                        "strike": strike,
                        "option_type": option_type
                    })
        except Exception:
            pass

    def parse_cryptopanic(self, data):
        results = []
        if "results" in data: results = data.get("results", [])
        elif "dbd" in data:
             comments = data.get("dbd", {}).get("comments", [])
             for c in comments:
                 if c.get("item"): results.append(c.get("item"))

        for post in results:
            title = post.get("title")
            currencies = [c.get("code") for c in post.get("currencies", [])]
            votes = post.get("votes", {})
            sentiment = "Neutral"
            if votes:
                 bullish = votes.get("bullish", 0)
                 bearish = votes.get("bearish", 0)
                 if bullish > bearish: sentiment = "Bullish"
                 elif bearish > bullish: sentiment = "Bearish"
            
            if title:
                self.save_news_csv({
                    "timestamp": time.time(),
                    "source": "CryptoPanic",
                    "title": title,
                    "amount": None,
                    "currency": "|".join(currencies),
                    "sentiment": sentiment
                })

    def parse_whale_alert(self, data):
        if isinstance(data, list):
            for item in data:
                if "transaction" in item or "hash" in item:
                    self.save_news_csv({
                        "timestamp": time.time(),
                        "source": "Whale/DexCheck",
                        "title": "Large Transaction Detected",
                        "amount": float(item.get("amount", 0)) or float(item.get("value", 0)),
                        "currency": item.get("symbol", "") or item.get("token", ""),
                        "sentiment": None
                    })

    def response(self, flow: mitmproxy.http.HTTPFlow):
        if not self.is_target(flow.request.pretty_url): return
        
        ctype = flow.response.headers.get("content-type", "").lower()
        if "json" in ctype:
            try:
                data = json.loads(flow.response.text)
                url = flow.request.pretty_url
                if "cryptopanic.com" in url: self.parse_cryptopanic(data)
                elif "dexcheck" in url or "whale" in url: self.parse_whale_alert(data)
            except: pass

    def websocket_message(self, flow: mitmproxy.http.HTTPFlow):
        if not self.is_target(flow.request.pretty_url): return

        try:
            message = flow.websocket.messages[-1]
            decoded = self.decode_message(message.content)
            if decoded:
                with open(self.debug_log, "a") as f: f.write(f"WS Frame from {flow.request.pretty_url} ({len(decoded)} chars)\n")
                data = json.loads(decoded)

                url = flow.request.pretty_url
                if "bybit.com" in url: self.parse_bybit(data)
                elif "deribit.com" in url: self.parse_deribit(data)
                elif "binance.com" in url: self.parse_binance(data)
        except: pass

addons = [
    CryptoParser()
]
