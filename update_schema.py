import psycopg2
from db_config import DB_CONFIG

def update_schema():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='dairy_customers' AND column_name='last_bill_amount'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE dairy_customers ADD COLUMN last_bill_amount REAL DEFAULT 0.0")
            print("Successfully added last_bill_amount column to dairy_customers.")
        else:
            print("Column last_bill_amount already exists.")
            
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error updating schema: {e}")

if __name__ == "__main__":
    update_schema()
