import sqlite3

DB_NAME = "shop_system.db"
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()

with open("full_schema.txt", "w") as f:
    for table_name in tables:
        table_name = table_name[0]
        f.write(f"--- Table: {table_name} ---\n")
        cur.execute(f"PRAGMA table_info({table_name});")
        columns = cur.fetchall()
        for col in columns:
            f.write(str(col) + "\n")
        
        cur.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}';")
        create_sql = cur.fetchone()[0]
        f.write(f"CREATE SQL: {create_sql}\n\n")

conn.close()
