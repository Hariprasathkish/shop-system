import sqlite3
import os

DB_NAME = 'shop_system.db'

def check():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(dairy_customers)")
    cols = cur.fetchall()
    print("Columns in dairy_customers:")
    for c in cols:
        print(f"Index: {c[0]}, Name: {c[1]}, Type: {c[2]}, NotNull: {c[3]}, Default: {c[4]}, PK: {c[5]}")
    conn.close()

if __name__ == "__main__":
    check()
