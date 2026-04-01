import sqlite3

DB_NAME = "shop_system.db"
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()

for table_name in tables:
    table_name = table_name[0]
    print(f"--- Table: {table_name} ---")
    cur.execute(f"PRAGMA table_info({table_name});")
    columns = cur.fetchall()
    for col in columns:
        print(col)
    print("\n")

conn.close()
