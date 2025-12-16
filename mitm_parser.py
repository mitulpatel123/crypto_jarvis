import mitmproxy.http
from mitmproxy import ctx
import re
from datetime import datetime
import json
import os
import sys
import zlib
import time
import threading
import queue
import traceback
import psycopg2 
from bs4 import BeautifulSoup # Added for better HTML parsing

# --- Config Import Hack for mitmdump ---
# Ensure we can import config.py from the same directory
sys.path.append(os.path.dirname(__file__))
import config

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
    "binance.com",
    "cointelegraph.com",
    "coindesk.com",
    "decrypt.co",
    "beincrypto.com",
    "thedefiant.io",
    "blockworks.co"
]

OUTPUT_FILE = "captured_data.jsonl"

try:
    import brotli
except ImportError:
    brotli = None

class DatabaseWriter(threading.Thread):
    def __init__(self, q):
        super().__init__()
        self.queue = q
        # Phase 7: Use Config Isolation
        self.db_uri = config.DB_URI 
        self.batch_size = config.BATCH_SIZE
        self.flush_interval = config.BATCH_INTERVAL
        self.daemon = True 
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
            with open(debug_path, "a") as f: f.write(f"[Writer] Connected to DB (Port {config.DB_PORT})\n")
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

                # Auto-flush
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

        # Phase 7: Fixed Schema Drift (Added 'source' column)
        if derivs:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(d['timestamp'])), 
                 d['symbol'], d.get('funding_rate', 0), d.get('open_interest', 0), d.get('turnover', 0), 
                 d.get('iv', 0), d.get('delta', 0), d.get('gamma', 0),
                 d.get('source'),  # Added source
                 d.get('expiry'), d.get('strike', 0), d.get('option_type')
             )).decode('utf-8') for d in derivs)
             cursor.execute("INSERT INTO derivatives_stats (time, symbol, funding_rate, open_interest, turnover, iv, delta, gamma, source, expiry, strike, option_type) VALUES " + args_str + " ON CONFLICT DO NOTHING")

        if news:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(n['timestamp'])), 
                 n['source'], n['title'], n['currency'], n['sentiment'], n['amount']
             )).decode('utf-8') for n in news)
             # Note: schema.sql has raw_data JSONB, but simple insert ignores it (default null) which is fine for now
             cursor.execute("INSERT INTO news_sentiment (time, source, title, currency, sentiment, amount) VALUES " + args_str + " ON CONFLICT DO NOTHING")

    def stop(self):
        self.running = False


class CryptoParser:
    def __init__(self):
        self.debug_log = os.path.join(os.path.dirname(__file__), "parser_debug.log")
        with open(self.debug_log, "w") as f:
            f.write("Parser initialized\n")
            
        # Initialize DB Writer Pipeline
        # Phase 7: Bounded Queue
        self.queue = queue.Queue(maxsize=config.QUEUE_MAX_SIZE)
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
        if not isinstance(content, bytes): return str(content)
        try: return content.decode('utf-8')
        except: pass
        try: return zlib.decompress(content, 16 + zlib.MAX_WBITS).decode('utf-8')
        except: pass
        try: return zlib.decompress(content, -zlib.MAX_WBITS).decode('utf-8')
        except: pass
        if brotli:
            try: return brotli.decompress(content).decode('utf-8')
            except: pass
        return None

    def push_to_queue(self, record_type, data):
        """Push structured data to DB Writer Queue"""
        payload = data.copy()
        payload['type'] = record_type
        try:
            self.queue.put(payload, block=False)
        except queue.Full:
            with open(self.debug_log, "a") as f: f.write("Queue Full, dropping packet\n")

    def save_csv(self, row):
        rtype = 'tick'
        if row.get('funding_rate') or row.get('iv'):
            rtype = 'derivative'
        self.push_to_queue(rtype, row)

    def save_news_csv(self, row):
        self.push_to_queue('news', row)

    def log_error(self, context, error):
        with open(self.debug_log, "a") as f: f.write(f"[{context}] Error: {error}\n")

    def parse_bybit(self, data):
        try:
            topic = data.get("topic", "")
            if any(x in topic for x in ["ticker", "trade", "mark_price", "book"]):
                payload = data.get("data", [])
                if isinstance(payload, dict): payload = [payload]
                
                for item in payload:
                    symbol = item.get("s", "Unknown")
                    price = item.get("c") or item.get("p") or item.get("p1")
                    bid = item.get("b1")
                    ask = item.get("a1")
                    volume = item.get("v") or item.get("q")
                    
                    side = None
                    if "m" in item:
                        side = "SELL" if item.get("m") else "BUY"
                    elif item.get("S"):
                        side = item.get("S")

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
        except Exception as e:
            self.log_error("Bybit Parse", e)

    def parse_binance(self, data):
        try:
            stream = data.get("stream", "")
            payload = data.get("data", {})
            
            if "forceOrder" in stream:
                o = payload.get("o", {})
                symbol = o.get("s")
                price = o.get("p")
                qty = o.get("q")
                if symbol and price:
                     self.save_csv({
                        "timestamp": time.time(),
                        "source": "Binance_Liq",
                        "symbol": symbol,
                        "price": float(price),
                        "bid": None, "ask": None,
                        "volume": float(qty),
                        "side": o.get("S"),
                        "open_interest": None, "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })

            elif "aggTrade" in stream:
                symbol = payload.get("s")
                price = payload.get("p")
                qty = payload.get("q")
                is_maker = payload.get("m") 
                if symbol and price:
                    self.save_csv({
                        "timestamp": float(payload.get("T", time.time()*1000)) / 1000,
                        "source": "Binance_Spot",
                        "symbol": symbol,
                        "price": float(price),
                        "bid": None, "ask": None,
                        "volume": float(qty),
                        "side": "SELL" if is_maker else "BUY",
                        "open_interest": None, "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })

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
                        "open_interest": None, "funding_rate": None, "turnover": None,
                        "iv": None, "delta": None, "gamma": None,
                        "expiry": None, "strike": None, "option_type": None
                    })
        except Exception as e:
            self.log_error("Binance Parse", e)

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
                
                expiry, strike, option_type = None, None, None
                try:
                    match = re.search(r'^[A-Z]+-(\d{1,2}[A-Z]{3}\d{2})-(\d+)-([CP])$', symbol)
                    if match:
                        date_str = match.group(1)
                        expiry = datetime.strptime(date_str, "%d%b%y").replace(tzinfo=None)
                        strike = float(match.group(2))
                        option_type = "CALL" if match.group(3) == "C" else "PUT"
                except: pass

                if symbol and price:
                    self.save_csv({
                        "timestamp": time.time(),
                        "source": "Deribit",
                        "symbol": symbol,
                        "price": float(price) if price else None,
                        "bid": float(item.get("best_bid_price")) if item.get("best_bid_price") else None,
                        "ask": float(item.get("best_ask_price")) if item.get("best_ask_price") else None,
                        "volume": float(item.get("stats", {}).get("volume")) if item.get("stats", {}).get("volume") else None,
                        "side": item.get("tick_direction"), 
                        "open_interest": float(item.get("open_interest")) if item.get("open_interest") else None,
                        "funding_rate": None, "turnover": None,
                        "iv": float(iv) if iv else None,
                        "delta": float(greeks.get("delta")) if greeks and greeks.get("delta") else None,
                        "gamma": float(greeks.get("gamma")) if greeks and greeks.get("gamma") else None,
                        "expiry": str(expiry) if expiry else None,
                        "strike": strike,
                        "option_type": option_type
                    })
        except Exception as e:
            self.log_error("Deribit Parse", e)

    def parse_cryptopanic(self, data):
        try:
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
        except Exception as e:
            self.log_error("CryptoPanic Parse", e)

    def parse_whale_alert(self, data):
        try:
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
        except Exception as e:
            self.log_error("WhaleAlert Parse", e)

    def parse_html_content(self, url, html_content):
        """Advanced HTML parsing for sites that server-side render"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 1. Decrypt.co Specific
            if "decrypt.co" in url:
                articles = soup.find_all('article')
                for article in articles[:5]: # Top 5 only
                    title_tag = article.find('h3') or article.find('h2')
                    if title_tag:
                        self.save_news_csv({
                            "timestamp": time.time(),
                            "source": "Decrypt",
                            "title": title_tag.get_text().strip(),
                            "amount": None,
                            "currency": "Crypto",
                            "sentiment": "Neutral"
                        })
                return

            # 2. BeInCrypto Specific
            if "beincrypto.com" in url:
                articles = soup.select('.story-card-text') 
                for article in articles[:5]:
                    title = article.get_text().strip()
                    if title:
                        self.save_news_csv({
                            "timestamp": time.time(),
                            "source": "BeInCrypto",
                            "title": title,
                            "amount": None,
                            "currency": "Crypto",
                            "sentiment": "Neutral"
                        })
                return

            # 3. Generic Fallback (Title Tag)
            title = soup.title.string if soup.title else ""
            if title:
                # Deduce source
                source = "GeneralNews"
                for d in TARGET_DOMAINS:
                    if d in url: source = d; break
                
                self.save_news_csv({
                     "timestamp": time.time(),
                     "source": source,
                     "title": title.strip(),
                     "amount": None,
                     "currency": "Crypto",
                     "sentiment": "Neutral"
                })

        except Exception as e:
            self.log_error("HTML Parse", e)

    def response(self, flow: mitmproxy.http.HTTPFlow):
        # DEBUG: Log all traffic to see what's happening
        with open(self.debug_log, "a") as f:
            f.write(f"Seen: {flow.request.pretty_url} | Type: {flow.response.headers.get('content-type', '')}\n")

        if not self.is_target(flow.request.pretty_url): return
        
        ctype = flow.response.headers.get("content-type", "").lower()
        
        try:
            if "json" in ctype:
                data = json.loads(flow.response.text)
                url = flow.request.pretty_url
                if "cryptopanic.com" in url: self.parse_cryptopanic(data)
                elif "dexcheck" in url or "whale" in url: self.parse_whale_alert(data)
                elif "bybit.com" in url and "api" in url: self.parse_bybit(data)
            
            elif "html" in ctype:
                # Handle generic news sites
                self.parse_html_content(flow.request.pretty_url, flow.response.text)
                
        except Exception as e:
             self.log_error("HTTP Response Parse", e)

    def websocket_message(self, flow: mitmproxy.http.HTTPFlow):
        if not self.is_target(flow.request.pretty_url): return

        try:
            message = flow.websocket.messages[-1]
            decoded = self.decode_message(message.content)
            if decoded:
                data = json.loads(decoded)
                url = flow.request.pretty_url
                if "bybit.com" in url: self.parse_bybit(data)
                elif "deribit.com" in url: self.parse_deribit(data)
                elif "binance.com" in url: self.parse_binance(data)
        except Exception as e:
            # We don't want to spam log for every malformed packet, but basic catching is good
            self.log_error("WS Message", e)

addons = [
    CryptoParser()
]
