import sqlite3
import os

DB_NAME = "shop_system.db"

def migrate():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Try adding the net_payable column
    try:
        cur.execute("ALTER TABLE dairy_customers ADD COLUMN net_payable REAL DEFAULT 0.0")
        print("Column 'net_payable' added successfully.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("Column 'net_payable' already exists.")
        else:
            print(f"Error: {e}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate()
