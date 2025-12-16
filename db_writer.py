import os
import time
import multiprocessing
import traceback
# import psycopg2 # Disabled until DB is ready

class DatabaseWriter(multiprocessing.Process):
    def __init__(self, queue, db_uri=None):
        super().__init__()
        self.queue = queue
        self.db_uri = db_uri
        self.batch_size = 100
        self.flush_interval = 1.0 # seconds
        self.running = True

    def run(self):
        debug_path = os.path.join(os.path.dirname(__file__), "writer_debug.log")
        with open(debug_path, "w") as f:
             f.write(f"[Writer] Process started. PID: {os.getpid()}\n")
        
        buffer = []
        last_flush = time.time()
        
        # Mock connection 
        # conn = psycopg2.connect(self.db_uri)
        # cursor = conn.cursor()

        while self.running:
            try:
                # Non-blocking fetch with small timeout
                try:
                    record = self.queue.get(timeout=0.1)
                    buffer.append(record)
                    with open(debug_path, "a") as f: f.write(f"Received record: {record.get('symbol')}\n")
                except multiprocessing.queues.Empty:
                    pass

                # Auto-flush on time or batch size
                now = time.time()
                if len(buffer) >= self.batch_size or (now - last_flush > self.flush_interval and buffer):
                    self.flush_batch(buffer)
                    buffer = []
                    last_flush = now
                
            except Exception as e:
                print(f"[DB Writer] Error loop: {e}")
                traceback.print_exc()
    
    def flush_batch(self, batch):
        log_msg = f"[DB Writer] Flushing batch of {len(batch)} records...\n"
        
        # Separate by table
        ticks = [r for r in batch if r['type'] == 'tick']
        derivs = [r for r in batch if r['type'] == 'derivative']
        news = [r for r in batch if r['type'] == 'news']
        
        if ticks: log_msg += f"   -> Inserted {len(ticks)} ticks into 'market_ticks'\n"
        if derivs: log_msg += f"   -> Inserted {len(derivs)} derivs into 'derivatives_stats'\n"
        if news: log_msg += f"   -> Inserted {len(news)} news into 'news_sentiment'\n"

        with open(os.path.join(os.path.dirname(__file__), "db_activity.log"), "a") as f:
            f.write(log_msg)

    def stop(self):
        self.running = False
