import sqlite3
import datetime

DB_NAME = "shop_system.db"
month_str = datetime.date.today().strftime("%Y-%m")

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

print(f"Checking dairy_payments for month: {month_str}")
cur.execute("SELECT customer_id, month, amount, payment_mode FROM dairy_payments WHERE month = ?", (month_str,))
rows = cur.fetchall()

if not rows:
    print("No payments found for this month.")
else:
    for row in rows:
        print(f"CID: {row[0]}, Month: {row[1]}, Amount: {row[2]}, Mode: {row[3]}")

conn.close()
