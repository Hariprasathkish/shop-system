import sqlite3
import os

DB_NAME = "shop_system.db"

def test_db():
    print(f"Testing access to {DB_NAME}...")
    if not os.path.exists(DB_NAME):
        print("Data base file does not exist!")
        return
    
    try:
        conn = sqlite3.connect(DB_NAME, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT name FROM dairy_customers LIMIT 1")
        row = cur.fetchone()
        print(f"Success! Found customer: {row[0] if row else 'None'}")
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_db()
