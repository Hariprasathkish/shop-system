import sqlite3

def upgrade():
    conn = sqlite3.connect('shop_system.db')
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE dairy_customers ADD COLUMN last_bill_generated_on TEXT")
        print("Successfully added last_bill_generated_on to dairy_customers.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("Column last_bill_generated_on already exists.")
        else:
            print(f"Error: {e}")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    upgrade()
