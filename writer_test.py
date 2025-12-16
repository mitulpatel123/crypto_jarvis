import threading
import queue
import time
import os
import io
import traceback
import psycopg2

class DatabaseWriter(threading.Thread):
    def __init__(self, q, db_uri=None):
        super().__init__()
        self.queue = q
        self.db_uri = db_uri or "postgres://postgres:password@127.0.0.1:5433/postgres"
        self.batch_size = 2
        self.flush_interval = 1.0 # seconds
        self.daemon = True # Auto-kill on exit
        self.running = True

    def run(self):
        print(f"[Writer] Process started. PID: {os.getpid()}")
        buffer = []
        last_flush = time.time()
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(self.db_uri)
            cursor = conn.cursor()
            print(f"[Writer] Connected to DB: {self.db_uri}")
            
            # DEBUG: List tables
            cursor.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
            tables = cursor.fetchall()
            print(f"[Writer] Visible tables: {tables}")

        except Exception as e:
             print(f"[Writer] DB CONNECT FAIL: {e}")

        while self.running:
            try:
                try:
                    record = self.queue.get(timeout=0.1)
                    buffer.append(record)
                    print(f"Received record: {record.get('symbol')}")
                except queue.Empty:
                    pass

                now = time.time()
                if len(buffer) >= self.batch_size or (now - last_flush > self.flush_interval and buffer):
                    if conn:
                        self.flush_batch(cursor, buffer)
                        conn.commit()
                    buffer = []
                    last_flush = now
                
            except Exception as e:
                print(f"Error: {e}")
                if conn: 
                    conn.rollback()
                    print("Rolled back transaction.")
                traceback.print_exc()
        
        if conn: conn.close()
    
    def flush_batch(self, cursor, batch):
        print(f"[DB Writer] Flushing batch of {len(batch)} records...")
        ticks = [r for r in batch if r['type'] == 'tick']
        if ticks:
             args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", (
                 time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t['timestamp'])), 
                 t['symbol'], t['price'], t['bid'], t['ask'], t['volume'], t['source']
             )).decode('utf-8') for t in ticks)
             cursor.execute("INSERT INTO market_ticks (time, symbol, price, bid, ask, volume, source) VALUES " + args_str)

    def stop(self):
        self.running = False

q = queue.Queue()
w = DatabaseWriter(q)
w.start()

# Push records
ts = time.time()
q.put({"type": "tick", "timestamp": ts, "symbol": "TEST1", "price": 100.0, "bid": 99, "ask": 101, "volume": 1, "source": "Test"})
q.put({"type": "tick", "timestamp": ts, "symbol": "TEST2", "price": 200.0, "bid": 199, "ask": 201, "volume": 2, "source": "Test"})
time.sleep(2)
w.stop()
w.join()
