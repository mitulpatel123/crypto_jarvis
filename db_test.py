import psycopg2
import sys

uri = "postgres://postgres:password@localhost:5432/postgres"

print(f"Connecting to {uri}...")
try:
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print("SUCCESS: Connected and ran SELECT 1")
    conn.close()
except Exception as e:
    print(f"FAILURE: {e}")
    sys.exit(1)
