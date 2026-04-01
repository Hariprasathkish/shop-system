import sqlite3
import json

DB_NAME = "shop_system.db"

def migrate():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    try:
        print("Starting migration for Multi-Product Support...")
        
        # 1. Create customer_products table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            product_name TEXT,
            default_qty REAL,
            price REAL,
            delivery_order INTEGER DEFAULT 0,
            FOREIGN KEY(customer_id) REFERENCES dairy_customers(id)
        )
        """)
        print("Created customer_products table.")

        # 2. Add product_id to dairy_logs if not exists
        cur.execute("PRAGMA table_info(dairy_logs)")
        columns = [row[1] for row in cur.fetchall()]
        if "product_id" not in columns:
            print("Adding product_id to dairy_logs...")
            cur.execute("ALTER TABLE dairy_logs ADD COLUMN product_id INTEGER")
        
        # 3. Migrate existing customer data to customer_products
        print("Migrating existing customer data...")
        cur.execute("SELECT id, product_name, default_qty, price_per_liter FROM dairy_customers")
        customers = cur.fetchall()
        
        for cust in customers:
            cid, p_names, qty, price = cust
            # p_names might be "Blue, Green" from previous simple implementation
            # or just "Blue"
            if p_names:
                products = [p.strip() for p in p_names.split(',')]
                for i, prod in enumerate(products):
                    # Check if already exists to avoid duplicates if run multiple times
                    cur.execute("SELECT id FROM customer_products WHERE customer_id=? AND product_name=?", (cid, prod))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO customer_products (customer_id, product_name, default_qty, price, delivery_order)
                            VALUES (?, ?, ?, ?, ?)
                        """, (cid, prod, qty if i==0 else 0, price, i)) # Only give default qty to first product for safety
        
        # 4. Attempt to link existing logs to products (Best Effort)
        # This is tricky because we don't know which product a log was for. 
        # For now, we will link to the *first* product of the customer.
        print("Linking existing logs to default products...")
        cur.execute("SELECT id, customer_id FROM dairy_logs WHERE product_id IS NULL")
        logs = cur.fetchall()
        for log in logs:
            lid, cid = log
            # Get first product for this customer
            cur.execute("SELECT id FROM customer_products WHERE customer_id=? ORDER BY id ASC LIMIT 1", (cid,))
            prod = cur.fetchone()
            if prod:
                cur.execute("UPDATE dairy_logs SET product_id=? WHERE id=?", (prod[0], lid))

        conn.commit()
        print("Migration successful.")
            
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
