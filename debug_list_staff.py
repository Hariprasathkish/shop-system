import sqlite3
DB_NAME = "shop_system.db"
try:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT * FROM delivery_staff")
    rows = cur.fetchall()
    print("Staff in DB:", rows)
    conn.close()
except Exception as e:
    print("Error:", e)
