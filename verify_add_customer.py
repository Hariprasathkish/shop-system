import sqlite3
import os

DB_NAME = 'shop_system.db'

def verify():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    print("Checking for most recent customer...")
    cur.execute("SELECT * FROM dairy_customers ORDER BY id DESC LIMIT 1")
    customer = cur.fetchone()
    
    if customer:
        print(f"Latest Customer: ID={customer[0]}, Name={customer[1]}, Billing={customer[11]}")
        
        print("Checking products for this customer...")
        cur.execute("SELECT * FROM customer_products WHERE customer_id=?", (customer[0],))
        products = cur.fetchall()
        for p in products:
            print(f"  Product: {p[2]}, Qty: {p[3]}, Price: {p[4]}")
    else:
        print("No customers found.")
        
    conn.close()

if __name__ == "__main__":
    verify()
