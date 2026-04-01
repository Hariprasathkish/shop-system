import sqlite3
import datetime
import os
import sys

# Add current dir to path to import app (if possible) or just mock the logic
DB_NAME = "shop_system.db"

def verify_pdf_generation():
    print("Starting PDF generation verification...")
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Check if we have any customers
    cur.execute("SELECT id, name, billing_type FROM dairy_customers LIMIT 1")
    customer = cur.fetchone()
    
    if not customer:
        print("No customers found in DB. Creating a dummy one.")
        cur.execute("INSERT INTO dairy_customers (name, billing_type) VALUES ('Test Customer', 'current_month')")
        customer_id = cur.lastrowid
        conn.commit()
    else:
        customer_id = customer[0]
        print(f"Testing with customer {customer[1]} (ID: {customer_id})")

    # Add a dummy product if none
    cur.execute("SELECT id FROM customer_products WHERE customer_id=?", (customer_id,))
    if not cur.fetchone():
        cur.execute("INSERT INTO customer_products (customer_id, product_name, default_qty, price) VALUES (?, 'Milk', 1.0, 50.0)", (customer_id,))
        conn.commit()

    conn.close()

    # Now we try to call the logic. Since we can't easily call the route without a Flask app context,
    # we'll use a trick: we'll import the function from app.py and run it in a request context.
    from app import app
    
    with app.test_request_context(method='POST', query_string={'month': '2026-02'}):
        # We need to mock the session
        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess['role'] = 'admin'
            
            print(f"Calling /dairy/generate_bill/{customer_id}...")
            response = client.post(f"/dairy/generate_bill/{customer_id}?month=2026-02")
            print("Response:", response.get_json())
            
            data = response.get_json()
            if data and data.get('status') == 'success':
                pdf_path = data.get('pdf_url').lstrip('/')
                if os.path.exists(pdf_path):
                    print(f"SUCCESS: PDF created at {pdf_path}")
                    print(f"File size: {os.path.getsize(pdf_path)} bytes")
                    return True
                else:
                    print(f"FAILURE: PDF file NOT found at {pdf_path}")
            else:
                print("FAILURE: API call failed.")
    
    return False

if __name__ == "__main__":
    try:
        if verify_pdf_generation():
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        print(f"ERROR during verification: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
