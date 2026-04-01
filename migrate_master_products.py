import sqlite3

def migrate():
    conn = sqlite3.connect("shop_system.db")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_master_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        default_price REAL DEFAULT 50.0
    )
    """)
    cur.execute("SELECT COUNT(*) FROM dairy_master_products")
    if cur.fetchone()[0] == 0:
        for p in ["Blue(tonned)", "Green(magic)", "Violet(delite)", "Orange(premium)", "Heritage(normal)", "Heritage(gold)"]:
            cur.execute("INSERT OR IGNORE INTO dairy_master_products (name, default_price) VALUES (?, ?)", (p, 50.0))
    conn.commit()
    conn.close()
    print("Migrated master products table.")

if __name__ == "__main__":
    migrate()
