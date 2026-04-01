import sqlite3
import os

DB_NAME = "shop_system.db"
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

print("Schema for dairy_customers:")
cur.execute("SELECT sql FROM sqlite_master WHERE name='dairy_customers'")
res = cur.fetchone()
if res:
    print(res[0])

print("\nSchema for dairy_extra_notes:")
cur.execute("SELECT sql FROM sqlite_master WHERE name='dairy_extra_notes'")
res = cur.fetchone()
if res:
    print(res[0])

conn.close()
