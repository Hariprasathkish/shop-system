import sqlite3
import calendar
import csv
import os
import json
import datetime

DB_NAME = "shop_system.db"

def verify_csv_logic(month_str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    year, month = map(int, month_str.split("-"))
    num_days = calendar.monthrange(year, month)[1]
    days = [f"{year}-{month:02d}-{d:02d}" for d in range(1, num_days + 1)]
    day_nums = [str(d) for d in range(1, num_days + 1)]

    # 1. Fetch Customers & Products
    cur.execute("""
        SELECT c.id, c.name, s.name as staff_name, c.delivery_order
        FROM dairy_customers c
        LEFT JOIN delivery_staff s ON c.delivery_staff_id = s.id
        ORDER BY c.delivery_order ASC, c.id ASC
    """)
    cust_rows = cur.fetchall()
    
    cur.execute("SELECT id, customer_id, product_name, default_qty, price FROM customer_products")
    prod_rows = cur.fetchall()
    prod_map = {}
    for p in prod_rows:
        cid = p[1]
        if cid not in prod_map: prod_map[cid] = []
        prod_map[cid].append({"id": p[0], "name": p[2], "qty": p[3], "price": p[4]})

    # 2. Fetch Logs for this month
    cur.execute("SELECT product_id, date, quantity FROM dairy_logs WHERE date LIKE ?", (f"{month_str}-%",))
    logs_raw = cur.fetchall()
    logs_map = {} # {pid: {date: qty}}
    for pid, d, qty in logs_raw:
        pid_str = str(pid)
        if pid_str not in logs_map: logs_map[pid_str] = {}
        logs_map[pid_str][d] = qty

    # 3. Fetch Extra Purchases
    cur.execute("SELECT customer_id, date, product_name, quantity, amount FROM dairy_extra_purchases WHERE date LIKE ?", (f"{month_str}-%",))
    extras_raw = cur.fetchall()
    extras_map = {} # {cid: [str]}
    for cid, edate, epname, eqty, eamt in extras_raw:
        if cid not in extras_map: extras_map[cid] = []
        extras_map[cid].append(f"{edate[8:]}: {epname}({eqty})")

    # 4. Fetch Payments
    cur.execute("SELECT customer_id, payment_date, amount, payment_mode FROM dairy_payments WHERE month = ?", (month_str,))
    payments_raw = cur.fetchall()
    payments_map = {} # {cid: [str]}
    for cid, pdate, pamt, pmode in payments_raw:
        if cid not in payments_map: payments_map[cid] = []
        payments_map[cid].append(f"{pdate[8:]}: Rs.{pamt}({pmode})")

    # 5. Build Result List
    matrix_data = []
    for c in cust_rows:
        cid, cname, sname, _ = c
        c_prods = prod_map.get(cid, [])
        
        for idx, p in enumerate(c_prods):
            pid_str = str(p["id"])
            atMap = logs_map.get(pid_str, {})
            
            row = {
                "Customer": cname if idx == 0 else "",
                "Staff": sname if idx == 0 else "",
                "Product": p["name"],
                "Default Qty": p["qty"]
            }
            
            total_qty = 0
            present_days = 0
            absent_days = 0
            
            for d in days:
                val = atMap.get(d)
                if val is not None:
                    q = float(val)
                    row[d[8:]] = q
                    total_qty += q
                    if q > 0: present_days += 1
                    else: absent_days += 1
                else:
                    row[d[8:]] = ""
            
            row["Monthly Total"] = total_qty
            row["Present (Days)"] = present_days
            row["Absent (Days)"] = absent_days
            
            if idx == 0:
                row["Extra Purchases"] = "; ".join(extras_map.get(cid, []))
                row["Payments"] = "; ".join(payments_map.get(cid, []))
            else:
                row["Extra Purchases"] = ""
                row["Payments"] = ""
            
            matrix_data.append(row)

    headers = ["Customer", "Staff", "Product", "Default Qty"] + day_nums + ["Monthly Total", "Present (Days)", "Absent (Days)", "Extra Purchases", "Payments"]
    
    test_csv = "test_report.csv"
    with open(test_csv, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in matrix_data:
            csv_row = {}
            for h in headers:
                if h in row: csv_row[h] = row[h]
                elif h.isdigit():
                    day_key = h.zfill(2)
                    csv_row[h] = row.get(day_key, "")
                else:
                    csv_row[h] = ""
            writer.writerow(csv_row)
            
    print(f"CSV generated: {test_csv}")
    print("Headers match:", headers[:10], "...")
    conn.close()

if __name__ == "__main__":
    # Get current month or some month that exists in DB
    verify_csv_logic("2026-03")
