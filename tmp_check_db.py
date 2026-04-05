import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", os.getenv("DB_DATABASE", "milk_management")),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

def check_snacks():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("\n--- snacks_menu full schema from information_schema ---")
        cur.execute("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'snacks_menu' ORDER BY ordinal_position")
        for col in cur.fetchall():
            print(col)
            
        print("\n--- snacks_menu sample content (all columns) ---")
        cur.execute("SELECT * FROM snacks_menu LIMIT 1")
        if cur.description:
            colnames = [desc[0] for desc in cur.description]
            print("Columns:", colnames)
            row = cur.fetchone()
            if row:
                print("Data:", row)
            else:
                print("No data found in snacks_menu.")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_snacks()
