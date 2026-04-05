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

def check_all_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("\n--- snacks_menu Image URLs ---")
        cur.execute("SELECT id, name, image_url FROM snacks_menu")
        rows = cur.fetchall()
        if not rows:
            print("No items found.")
        for row in rows:
            print(f"ID: {row[0]} | Name: {row[1]} | URL: {row[2]}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_all_data()
