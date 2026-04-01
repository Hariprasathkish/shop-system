import sqlite3
import os

DB_NAME = 'shop_system.db'

def check_schema():
    if not os.path.exists(DB_NAME):
        output = f"Error: {DB_NAME} not found."
    else:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        
        output = ""
        tables = ['dairy_extra_notes', 'dairy_logs', 'dairy_payments']
        for table in tables:
            output += f"\nSchema for {table}:\n"
            cur.execute(f"PRAGMA table_info({table})")
            columns = cur.fetchall()
            for col in columns:
                output += str(col) + "\n"
        conn.close()
    
    with open('schema_output.txt', 'w') as f:
        f.write(output)
    print("Schema written to schema_output.txt")

if __name__ == "__main__":
    check_schema()
