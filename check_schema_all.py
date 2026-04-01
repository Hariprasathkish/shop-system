import sqlite3

def get_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name in tables:
        print(f"Table: {table_name[0]}")
        cursor.execute(f"PRAGMA table_info({table_name[0]});")
        info = cursor.fetchall()
        for col in info:
            print(f"  {col[1]} ({col[2]})")
    conn.close()

print("--- database.db ---")
get_schema('database.db')
print("\n--- shop_system.db ---")
get_schema('shop_system.db')
