import sqlite3, pprint
conn = sqlite3.connect('shop_system.db')
cur = conn.cursor()
customers_schema = cur.execute('PRAGMA table_info(dairy_customers)').fetchall()
logs_schema = cur.execute('PRAGMA table_info(dairy_logs)').fetchall()
with open("schema_utf8.txt", "w", encoding="utf-8") as f:
    f.write("Customers schema:\n")
    for row in customers_schema:
        f.write(str(row) + "\n")
    f.write("\nLogs schema:\n")
    for row in logs_schema:
        f.write(str(row) + "\n")
