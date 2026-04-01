import sqlite3
import datetime
from app import app
import os

def setup_test_customer(customer_id, net_payable, last_gen_date):
    conn = sqlite3.connect('shop_system.db')
    cur = conn.cursor()
    cur.execute("UPDATE dairy_customers SET net_payable=?, last_bill_generated_on=? WHERE id=?", 
               (net_payable, last_gen_date, customer_id))
    conn.commit()
    conn.close()
    print(f"Setup customer {customer_id}: net_payable={net_payable}, last_gen={last_gen_date}")

def add_payment(customer_id, amount, date):
    conn = sqlite3.connect('shop_system.db')
    cur = conn.cursor()
    cur.execute("INSERT INTO dairy_payments (customer_id, month, payment_date, amount, payment_mode) VALUES (?, ?, ?, ?, ?)",
               (customer_id, '2026-03', date, amount, 'Cash'))
    conn.commit()
    conn.close()
    print(f"Added payment: {amount} on {date}")

def run_billing_test(customer_id):
    with app.test_request_context():
        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess['role'] = 'admin'
                sess['user_id'] = 1
            
            print(f"\n--- Generating Bill for Customer {customer_id} ---")
            resp = client.post(f"/dairy/generate_bill/{customer_id}?month=2026-03")
            data = resp.get_json()
            if data['status'] == 'success':
                print(f"Bill Generated! Amount: {data['bill_amount']}")
                print(f"Period: {data['period']}")
                print(f"Scan Window: {data['scan_window']}")
                return data['bill_amount']
            else:
                print(f"Error: {data['message']}")
                return None

if __name__ == "__main__":
    CID = 19 # Sabari
    # 1. Start with 1000 arrears, last generated yesterday
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    setup_test_customer(CID, 1000.0, yesterday)
    
    # 2. Run bill - should show 1000 + some current
    amt1 = run_billing_test(CID)
    
    # 3. Add a payment of 400 TODAY (after last generation)
    today = datetime.date.today().isoformat()
    add_payment(CID, 400.0, today)
    
    # 4. Run bill again - arrears should be 1000 - 400 = 600
    # Wait, if we run it again today, the scan window starts from last_gen+1 (which is today).
    # Since today's bill generation sets last_bill_generated_on to Today, 
    # and the next bill will scan from Tomorrow.
    # To test the "payments since last" logic, I need to:
    # Set last_gen to 2 days ago.
    # Add payment 1 day ago.
    # Run bill today.
    
    print("\n--- Phase 2: Testing payment deduction from arrears ---")
    two_days_ago = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()
    one_day_ago = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    
    setup_test_customer(CID, 1000.0, two_days_ago)
    add_payment(CID, 400.0, one_day_ago)
    
    amt2 = run_billing_test(CID)
    print(f"\nFinal Check: Bill 2 should have 600 arrears + current charges.")
