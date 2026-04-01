import sqlite3
import datetime

DB_NAME = "shop_system.db"
month_str = datetime.date.today().strftime("%Y-%m")

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

print(f"Clearing dairy_payments for month: {month_str}")
cur.execute("DELETE FROM dairy_payments WHERE month = ?", (month_str,))
conn.commit()
print(f"Deleted {cur.rowcount} records.")

conn.close()
