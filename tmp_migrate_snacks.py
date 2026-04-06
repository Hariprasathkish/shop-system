import psycopg2
import os
from dotenv import load_dotenv
from db_config import DB_CONFIG

load_dotenv()

def migrate():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Adding unit_size column...")
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS unit_size TEXT DEFAULT '1kg'")
        
        print("Adding is_bulk column...")
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS is_bulk BOOLEAN DEFAULT TRUE")
        
        print("Updating existing items...")
        # Simple heuristic: if name contains 'g' but not 'kg', set is_bulk to False and unit_size to those grams
        cur.execute("UPDATE snacks_menu SET is_bulk = FALSE, unit_size = '200g' WHERE name ILIKE '%200g%'")
        cur.execute("UPDATE snacks_menu SET is_bulk = FALSE, unit_size = '500g' WHERE name ILIKE '%500g%'")
        cur.execute("UPDATE snacks_menu SET is_bulk = FALSE, unit_size = 'half kg' WHERE name ILIKE '%half kg%'")
        
        conn.commit()
        cur.close()
        conn.close()
        print("Migration successful!")
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate()
