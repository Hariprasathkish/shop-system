import sqlite3
import datetime
from app import app

def test_bill(customer_id, month_str, expected_type):
    print(f"\n--- Testing {expected_type} (Customer {customer_id}) for {month_str} ---")
    
    with app.test_request_context(f"/dairy/generate_bill/{customer_id}?month={month_str}", method="POST"):
        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess['role'] = 'admin'
                sess['user_id'] = 1
                
            response = client.post(f"/dairy/generate_bill/{customer_id}?month={month_str}")
            data = response.get_json()
            
            if data and data.get('status') == 'success':
                print(f"Success! Bill Amount: {data.get('bill_amount')}, Period: {data.get('period')}")
                pdf_path = "." + data['pdf_url']
                print(f"PDF Generated at: {pdf_path}")
            else:
                print(f"Failed: {data}")

# Test 1: Reservation (Nanthini, 18)
test_bill(18, "2026-03", "Reservation")

# Test 2: Month End (Pandiyan, 21)
test_bill(21, "2026-03", "Month End")
