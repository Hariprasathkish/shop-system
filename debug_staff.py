import sqlite3
DB_NAME = "shop_system.db"
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()
try:
    cur.execute("INSERT INTO delivery_staff (name, username, password) VALUES ('Test Staff', 'test', '1234')")
    print("Inserted Test Staff")
except Exception as e:
    print(e)
conn.commit()
cur.execute("SELECT * FROM delivery_staff")
print(cur.fetchall())
conn.close()
