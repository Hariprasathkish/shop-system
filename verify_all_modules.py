import requests
import sqlite3
import os

BASE_URL = "http://127.0.0.1:5000"
DB_NAME = "shop_system.db"

def check_db():
    print("--- Database Integrity Check ---")
    if not os.path.exists(DB_NAME):
        print(f"FAILED: {DB_NAME} does not exist.")
        return False
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    
    required_tables = [
        'admin_users', 'snacks_menu', 'snacks_stock_in', 'snacks_bills', 'snacks_bill_items',
        'snacks_sales', 'dairy_customers', 'dairy_logs', 'delivery_staff', 'attendance_requests',
        'dairy_extra_notes', 'dairy_payments', 'customer_products', 'dairy_extra_purchases',
        'dairy_master_products', 'staff_payroll'
    ]
    
    missing = []
    for t in required_tables:
        if t not in tables:
            missing.append(t)
    
    if missing:
        print(f"FAILED: Missing tables: {', '.join(missing)}")
    else:
        print("SUCCESS: All required tables exist.")
    
    cur.execute("SELECT COUNT(*) FROM admin_users")
    admin_count = cur.fetchone()[0]
    print(f"Admin users found: {admin_count}")
    
    conn.close()
    return not missing

def check_routes():
    print("\n--- Route Accessibility Check ---")
    session = requests.Session()
    
    # 1. Login
    print("Attempting Admin Login...")
    try:
        login_resp = session.post(f"{BASE_URL}/login", data={
            'role': 'admin',
            'admin_username': 'admin',
            'admin_password': '1234'
        }, allow_redirects=True)
    except Exception as e:
        print(f"CRITICAL: Failed to connect to server: {e}")
        return

    if 'admin' in login_resp.url:
        print("SUCCESS: Login successful.")
    else:
        print(f"FAILED: Login failed (Current URL: {login_resp.url})")
        return

    # 2. Check Pages
    pages = [
        '/admin',
        '/admin/account',
        '/snacks',
        '/snacks/billing',
        '/snacks/inventory',
        '/snacks/accounts',
        '/dairy',
        '/dairy/attendance',
        '/dairy/billing',
        '/dairy/accounts',
        '/dairy/staff',
        '/dairy/customers',
        '/api/stock/items',
        '/api/stock/history'
    ]
    
    for page in pages:
        try:
            r = session.get(f"{BASE_URL}{page}", allow_redirects=True)
            status = "OK" if r.status_code == 200 else f"FAIL ({r.status_code})"
            
            # Check for common error indicators in HTML
            error_msg = ""
            lower_text = r.text.lower()
            if "templatesyntaxerror" in lower_text or "internal server error" in lower_text or "jinja2" in lower_text:
                status = "FAILED (Server/Jinja Error)"
                # Extract some context if possible
                if "error" in lower_text:
                    start = lower_text.find("error")
                    error_msg = f" - Context: {r.text[max(0, start-50):start+150]}"
            
            print(f"{page.ljust(20)}: {status}{error_msg}")
        except Exception as e:
            print(f"{page.ljust(20)}: EXCEPTION - {e}")

if __name__ == "__main__":
    db_ok = check_db()
    check_routes()
