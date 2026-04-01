import sqlite3
import os

DB_NAME = "shop_system.db"
if os.path.exists(DB_NAME):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(snacks_menu)")
    columns = cur.fetchall()
    print("Schema for snacks_menu (cid, name, type, notnull, dflt_value, pk):")
    for col in columns:
        print(col)
    
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='snacks_menu'")
    sql = cur.fetchone()
    print("\nCREATE statement:")
    print(sql[0] if sql else "Not found")
    conn.close()
else:
    print(f"{DB_NAME} not found")
