import sqlite3
import datetime
from app import app

def setup_test_data():
    conn = sqlite3.connect('shop_system.db')
    cur = conn.cursor()
    
    # Ensure a test customer exists with a memorized date
    cur.execute("INSERT OR IGNORE INTO dairy_customers (id, name, billing_type, last_bill_generated_on) VALUES (888, 'Reset Test', 'current_month', '2026-03-31')")
    conn.commit()
    conn.close()

def test_reset():
    setup_test_data()
    print("\n--- Testing Reset Functionality ---")
    
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['role'] = 'admin'
        
        # 1. Check current status
        conn = sqlite3.connect('shop_system.db')
        cur = conn.cursor()
        date_before = cur.execute("SELECT last_bill_generated_on FROM dairy_customers WHERE id=888").fetchone()[0]
        conn.close()
        print(f"Date before reset: {date_before}")
        
        # 2. Call reset route
        response = client.post("/dairy/reset_billing/888")
        data = response.get_json()
        print(f"API Response: {data}")
        
        # 3. Check status after reset
        conn = sqlite3.connect('shop_system.db')
        cur = conn.cursor()
        date_after = cur.execute("SELECT last_bill_generated_on FROM dairy_customers WHERE id=888").fetchone()[0]
        conn.close()
        print(f"Date after reset: {date_after}")
        
        if data['status'] == 'success' and date_after is None:
            print("--- RESET TEST PASSED ---")
        else:
            print("--- RESET TEST FAILED ---")

if __name__ == "__main__":
    test_reset()
