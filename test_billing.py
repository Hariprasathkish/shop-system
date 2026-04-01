import sqlite3
import datetime
import pdfplumber
import os
from app import app, generate_dairy_bill

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
                print(f"Reading PDF: {pdf_path}")
                
                if os.path.exists(pdf_path):
                    with pdfplumber.open(pdf_path) as pdf:
                        page = pdf.pages[0]
                        text = page.extract_text()
                        print("\nExtract from PDF top:")
                        # Print first 500 chars to see header, period, and summary
                        print(text[:800])
                else:
                    print("PDF file not found!")
            else:
                print(f"Failed: {data}")

# Test 1: Reservation (Nanthini, 18)
test_bill(18, "2026-03", "Reservation")

# Test 2: Month End (Pandiyan, 21)
test_bill(21, "2026-03", "Month End")

# Test 3: Current Month (Customer 1 - assuming Current Month)
test_bill(1, "2026-03", "Current Month")

