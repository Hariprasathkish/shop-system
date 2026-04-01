import sqlite3
import os

DB_NAME = 'shop_system.db'

def check_schema():
    if not os.path.exists(DB_NAME):
        print(f"Error: {DB_NAME} not found.")
        return
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    
    for (table,) in tables:
        print(f"\n--- {table} ---")
        cur.execute(f"PRAGMA table_info({table})")
        columns = cur.fetchall()
        for col in columns:
            print(col)
            
    conn.close()

if __name__ == "__main__":
    check_schema()
