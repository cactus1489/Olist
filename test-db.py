import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('.env.local')
url = os.getenv('DATABASE_URL')

print(f"Connecting with Python to: {url.split('@')[1]}")

try:
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    cur.execute("SELECT current_database(), now()")
    row = cur.fetchone()
    print(f"✅ Python Connection Success: {row}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Python Connection Failed: {e}")
