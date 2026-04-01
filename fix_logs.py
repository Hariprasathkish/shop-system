import sqlite3
import os

DB_NAME = "shop_system.db"

def fix_logs():
    if not os.path.exists(DB_NAME):
        print("DB not found")
        return
        
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # 1. Ensure product_id exists
    try:
        cur.execute("ALTER TABLE dairy_logs ADD COLUMN product_id INTEGER")
        print("Added product_id col")
    except:
        pass
        
    # 2. Update NULL product_id to the first product of the customer
    print("Updating NULL product IDs...")
    cur.execute("""
        UPDATE dairy_logs 
        SET product_id = (
            SELECT id FROM customer_products 
            WHERE customer_id = dairy_logs.customer_id 
            LIMIT 1
        )
        WHERE product_id IS NULL 
        OR product_id = 0
    """)
    
    rowcount = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Update complete. Fixed {rowcount} rows.")

if __name__ == "__main__":
    fix_logs()
