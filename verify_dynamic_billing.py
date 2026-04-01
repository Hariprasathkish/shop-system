import sqlite3
import datetime
from app import app

def setup_test_data():
    conn = sqlite3.connect('shop_system.db')
    cur = conn.cursor()
    
    # Ensure a test customer exists
    cur.execute("INSERT OR IGNORE INTO dairy_customers (id, name, billing_type, service_charge) VALUES (999, 'Test Dynamic', 'current_month', 10.0)")
    cur.execute("INSERT OR IGNORE INTO customer_products (customer_id, product_name, default_qty, price) VALUES (999, 'Test Milk', 1.0, 50.0)")
    pid = cur.execute("SELECT id FROM customer_products WHERE customer_id=999").fetchone()[0]
    
    # Clear previous logs for this test
    cur.execute("DELETE FROM dairy_logs WHERE customer_id=999")
    cur.execute("UPDATE dairy_customers SET last_bill_date=NULL, last_bill_generated_on=NULL WHERE id=999")
    
    # Add logs: March 1st (Present), March 2nd (Absent), March 5th (Present)
    cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, quantity) VALUES (999, ?, '2026-03-01', 1.0)", (pid,))
    cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, quantity) VALUES (999, ?, '2026-03-02', 0.0)", (pid,))
    cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, quantity) VALUES (999, ?, '2026-03-05', 1.0)", (pid,))
    
    conn.commit()
    conn.close()

def run_gen(customer_id, gen_date):
    print(f"\n--- Generating Bill for {gen_date} ---")
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['role'] = 'admin'
        
        response = client.post(f"/dairy/generate_bill/{customer_id}?generation_date={gen_date}")
        data = response.get_json()
        if data['status'] == 'success':
            print(f"Success! Period: {data['period']}, Amount: {data['bill_amount']}")
            return data
        else:
            print(f"Failed: {data['message']}")
            return None

def verify():
    setup_test_data()
    
    # Gen 1: Up to March 3rd
    # Period should be March 1 (fallback) to March 3
    # Expected consumption: March 1 (1.0), March 2 (0.0), March 3 (default 1.0) -> Total 2.0
    # Expected absent: March 2 (1.0) -> Deduct 50.0
    # Total: (2 * 50) + (10 * 1) - 50 = 60.0
    res1 = run_gen(999, '2026-03-03')
    
    # Gen 2: Up to March 6th
    # Period should be March 4 to March 6
    # Expected consumption: March 4 (default 1.0), March 5 (1.0), March 6 (default 1.0) -> Total 3.0
    # Expected absent: 0
    # Total: (3 * 50) + (10 * 1) - 0 = 160.0
    res2 = run_gen(999, '2026-03-06')
    
    print("\n--- Summary ---")
    if res1 and "01/03/26 - 03/03/26" in res1['period']:
        print("Test 1 Period Passed")
    else:
        print(f"Test 1 Period Failed: {res1['period'] if res1 else 'N/A'}")

    if res2 and "04/03/26 - 06/03/26" in res2['period']:
        print("Test 2 Period Passed")
    else:
        print(f"Test 2 Period Failed: {res2['period'] if res2 else 'N/A'}")

if __name__ == "__main__":
    verify()
