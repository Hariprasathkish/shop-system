from flask import Flask, render_template, request, redirect, session, send_file, jsonify, flash
import datetime
import calendar
import csv
import os
import json
import qrcode
import barcode
from barcode.writer import ImageWriter
import io
import base64
import werkzeug.utils
from werkzeug.security import generate_password_hash, check_password_hash
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv
from db_config import DB_CONFIG
import psycopg2
import cloudinary
import cloudinary.uploader
import cloudinary.api

# Load environment variables
load_dotenv()

# Cloudinary configuration
CLOUDINARY_URL = os.getenv("CLOUDINARY_URL")
if CLOUDINARY_URL:
    print(f"DEBUG: Cloudinary URL detected: {CLOUDINARY_URL[:20]}...") 
    cloudinary.config(secure=True)
else:
    print("CRITICAL: CLOUDINARY_URL NOT FOUND in environment variables!")
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

UPLOAD_FOLDER_SNACKS = os.path.join('static', 'uploads', 'snacks')
os.makedirs(UPLOAD_FOLDER_SNACKS, exist_ok=True)
import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# Force absolute paths for templates and static to prevent Render/Gunicorn resolution issues
app = Flask(__name__, 
            template_folder=os.path.join(BASE_DIR, 'templates'),
            static_folder=os.path.join(BASE_DIR, 'static'))
app.secret_key = os.getenv("SECRET_KEY", "fallback_secret_for_local_dev")

# CSRF Protection
app.config['WTF_CSRF_ENABLED'] = False
csrf = CSRFProtect(app)

# Rate Limiter
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

# Session config for 'Remember Me'
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=30)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False # Disable for local HTTP testing
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://unpkg.com; style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://fonts.googleapis.com; font-src 'self' https://cdnjs.cloudflare.com https://fonts.gstatic.com; img-src 'self' data: https://upload.wikimedia.org https://res.cloudinary.com;"
    return response





# ------------------ DATABASE INIT ------------------
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Admin Users Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS admin_users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        name TEXT NOT NULL
    )
    """)
    
    # Ensure default admin exists
    try:
        cur.execute("SELECT id FROM admin_users WHERE username='admin'")
        if not cur.fetchone():
            hashed_pw = generate_password_hash("1234")
            cur.execute("INSERT INTO admin_users (username, password, name) VALUES ('admin', %s, 'Administrator')", (hashed_pw,))
            conn.commit()
    except Exception as e:
        print("Error ensuring default admin:", e)

    # Snacks Tables - Updated for supermarket billing
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snacks_menu (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        purchase_price REAL NOT NULL DEFAULT 0,
        retail_price REAL NOT NULL DEFAULT 0,
        wholesale_price REAL NOT NULL DEFAULT 0,
        stock INTEGER NOT NULL DEFAULT 0,
        price REAL,
        image_url TEXT
    )
    """)

    # Migrate old snacks_menu if needed (PostgreSQL syntax)
    try:
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS purchase_price REAL NOT NULL DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS retail_price REAL NOT NULL DEFAULT 0")
        cur.execute("UPDATE snacks_menu SET retail_price = price WHERE retail_price = 0 AND price IS NOT NULL")
    except: pass
    try:
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS wholesale_price REAL NOT NULL DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE snacks_menu ADD COLUMN IF NOT EXISTS image_url TEXT")
    except: pass

    # Inward Stock from manufacturer
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snacks_stock_in (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL,
        qty INTEGER NOT NULL,
        remaining_qty INTEGER NOT NULL DEFAULT 0,
        purchase_price REAL NOT NULL DEFAULT 0,
        supplier TEXT,
        date DATE DEFAULT CURRENT_DATE,
        notes TEXT
    )
    """)
    # FIFO Migration
    try:
        cur.execute("ALTER TABLE snacks_stock_in ADD COLUMN IF NOT EXISTS remaining_qty INTEGER NOT NULL DEFAULT 0")
        cur.execute("UPDATE snacks_stock_in SET remaining_qty = qty WHERE remaining_qty = 0")
    except: pass

    # Bill sessions (each checkout = 1 bill)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snacks_bills (
        id SERIAL PRIMARY KEY,
        bill_mode TEXT NOT NULL DEFAULT 'retail',
        subtotal REAL NOT NULL DEFAULT 0,
        discount REAL NOT NULL DEFAULT 0,
        grand_total REAL NOT NULL DEFAULT 0,
        customer_name TEXT DEFAULT '',
        payment_mode TEXT DEFAULT 'Cash',
        payment_status TEXT DEFAULT 'Paid',
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    try:
        cur.execute("ALTER TABLE snacks_bills ADD COLUMN IF NOT EXISTS payment_status TEXT DEFAULT 'Paid'")
    except: pass

    # Bill line items
    cur.execute("""
    CREATE TABLE IF NOT EXISTS snacks_bill_items (
        id SERIAL PRIMARY KEY,
        bill_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        item_name TEXT NOT NULL,
        qty INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        cost_price REAL NOT NULL DEFAULT 0,
        total REAL NOT NULL
    )
    """)
    try:
        cur.execute("ALTER TABLE snacks_bill_items ADD COLUMN IF NOT EXISTS cost_price REAL NOT NULL DEFAULT 0")
    except: pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS snacks_sales (
        id SERIAL PRIMARY KEY,
        item_id INTEGER,
        qty INTEGER,
        total REAL,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Dairy Tables — created in dependency order

    # 1. delivery_staff (no deps)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS delivery_staff (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        name TEXT
    )
    """)

    # Ensure at least one default staff exists
    try:
        cur.execute("SELECT id FROM delivery_staff WHERE username='staff1'")
        if not cur.fetchone():
            hashed_pw = generate_password_hash("1234")
            cur.execute("INSERT INTO delivery_staff (username, password, name) VALUES ('staff1', %s, 'Default Delivery')", (hashed_pw,))
            conn.commit()
    except Exception as e:
        print("Error ensuring default staff:", e)

    # 2. dairy_customers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_customers (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        phone TEXT,
        address TEXT,
        product_name TEXT DEFAULT 'Milk',
        default_qty REAL DEFAULT 1.0,
        price_per_liter REAL DEFAULT 50.0,
        service_charge REAL DEFAULT 0.0,
        delivery_staff_id INTEGER,
        password TEXT DEFAULT '1234',
        delivery_order INTEGER DEFAULT 9999,
        billing_type TEXT DEFAULT 'current_month',
        last_bill_date DATE,
        last_bill_generated_on TEXT,
        last_bill_amount REAL DEFAULT 0.0,
        net_payable REAL DEFAULT 0.0,
        email TEXT,
        custom_id TEXT,
        FOREIGN KEY(delivery_staff_id) REFERENCES delivery_staff(id)
    )
    """)
    # Migration/Updates for dairy_customers
    # We use separate try blocks for each to ensure one failure doesn't block others
    for cmd in [
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS last_bill_amount REAL DEFAULT 0.0",
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS last_bill_generated_on TEXT",
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS net_payable REAL DEFAULT 0.0",
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS last_bill_date DATE",
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS email TEXT",
        "ALTER TABLE dairy_customers ADD COLUMN IF NOT EXISTS custom_id TEXT",
        "ALTER TABLE dairy_customers ALTER COLUMN last_bill_date TYPE DATE USING (NULLIF(last_bill_date, '')::DATE)"
    ]:
        try:
            cur.execute(cmd)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"DEBUG: Migration dairy_customers command failed: {cmd} | Error: {e}")

    # 3. customer_products (depends on dairy_customers)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_products (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        product_name TEXT,
        default_qty REAL,
        price REAL,
        delivery_order INTEGER DEFAULT 0,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id)
    )
    """)

    # 4. dairy_logs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_logs (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        date DATE DEFAULT CURRENT_DATE,
        time_slot TEXT,
        quantity REAL,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id),
        FOREIGN KEY(product_id) REFERENCES customer_products(id)
    )
    """)
    try:
        # Cast date to DATE if it was TEXT
        cur.execute("ALTER TABLE dairy_logs ALTER COLUMN date TYPE DATE USING (date::DATE)")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"DEBUG: Migration dairy_logs date cast failed: {e}")
    # 5. attendance_requests
    cur.execute("""
    CREATE TABLE IF NOT EXISTS attendance_requests (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        staff_id INTEGER,
        log_id INTEGER,
        new_date DATE,
        new_time_slot TEXT,
        new_quantity REAL,
        product_name TEXT,
        reason TEXT,
        status TEXT DEFAULT 'Pending',
        admin_response TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id),
        FOREIGN KEY(staff_id) REFERENCES delivery_staff(id)
    )
    """)
    # Migration for attendance_requests
    for cmd in [
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS product_name TEXT",
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS staff_id INTEGER",
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS admin_response TEXT",
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS log_id INTEGER",
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS new_date DATE",
        "ALTER TABLE attendance_requests ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE attendance_requests ALTER COLUMN new_date TYPE DATE USING (NULLIF(new_date, '')::DATE)",
        "ALTER TABLE attendance_requests ALTER COLUMN created_at TYPE TIMESTAMP USING (created_at::TIMESTAMP)"
    ]:
        try:
            cur.execute(cmd)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"DEBUG: Migration attendance_requests command failed: {cmd} | Error: {e}")


    # 6.5 extra_purchases
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_extra_purchases (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        date DATE DEFAULT CURRENT_DATE,
        product_name TEXT,
        quantity REAL,
        rate REAL,
        amount REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id)
    )
    """)
    try:
        cur.execute("ALTER TABLE dairy_extra_purchases ALTER COLUMN date TYPE DATE USING (date::DATE)")
    except: pass
    conn.commit()

    # 7. Legacy Data Migration for customer_products
    try:
        # If a customer has NO entries in customer_products, take their legacy data from dairy_customers
        cur.execute("""
            INSERT INTO customer_products (customer_id, product_name, default_qty, price)
            SELECT id, product_name, default_qty, price_per_liter 
            FROM dairy_customers 
            WHERE id NOT IN (SELECT DISTINCT customer_id FROM customer_products)
        """)
        
        # Update dairy_logs where product_id is NULL to point to the first product_id for that customer
        cur.execute("""
            UPDATE dairy_logs l
            SET product_id = (SELECT MIN(id) FROM customer_products p WHERE p.customer_id = l.customer_id)
            WHERE l.product_id IS NULL AND EXISTS (SELECT 1 FROM customer_products p WHERE p.customer_id = l.customer_id)
        """)
        conn.commit()
    except Exception as e:
        print(f"DEBUG: Migration product_id sync error: {e}")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_extra_notes (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        month TEXT,
        notes TEXT,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_payments (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        month TEXT,
        payment_date DATE,
        amount REAL,
        payment_mode TEXT,
        FOREIGN KEY(customer_id) REFERENCES dairy_customers(id)
    )
    """)
    try:
        cur.execute("ALTER TABLE dairy_payments ALTER COLUMN payment_date TYPE DATE USING (payment_date::DATE)")
    except: pass
    conn.commit()



    cur.execute("""
    CREATE TABLE IF NOT EXISTS dairy_master_products (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        default_price REAL NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS staff_payroll (
        id SERIAL PRIMARY KEY,
        staff_id INTEGER NOT NULL,
        month TEXT NOT NULL,
        base_salary REAL DEFAULT 0,
        commission REAL DEFAULT 0,
        deductions REAL DEFAULT 0,
        bonus REAL DEFAULT 0,
        total_paid REAL DEFAULT 0,
        payment_date DATE,
        payment_mode TEXT,
        notes TEXT,
        FOREIGN KEY(staff_id) REFERENCES delivery_staff(id)
    )
    """)
    try:
        cur.execute("ALTER TABLE staff_payroll ALTER COLUMN payment_date TYPE DATE USING (payment_date::DATE)")
    except: pass
    conn.commit()

    conn.commit()
    conn.close()

# init_db() - Moved to main block

# ------------------ HOME / LOGIN ------------------
@app.route("/")
def home():
    # Force authentication page by default, even if session exists
    # If the user wants to go to dashboard, they can click a link or we can add a 'Continue' button on login
    return render_template("login.html", hide_menu=True)

@app.route("/login", methods=["POST"])
@limiter.limit("5 per minute")
def login():
    role = request.form.get("role")
    remember_me = request.form.get("remember_me")
    
    if remember_me:
        session.permanent = True
    else:
        session.permanent = False
        
    if role == "admin":
        username = request.form.get("admin_username")
        password = request.form.get("admin_password")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, username, password, name FROM admin_users WHERE username=%s", (username,))
        admin = cur.fetchone()
        conn.close()
        
        if admin and check_password_hash(admin[2], password):
            session["role"] = "admin"
            session["admin_id"] = admin[0]
            session["admin_name"] = admin[3]
            return redirect("/admin")
        else:
            flash("Invalid Admin Username or Password", "danger")
            return redirect("/")
    
    elif role == "customer":
        login_identifier = request.form.get("customer_id")
        password = request.form.get("customer_password")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name, password FROM dairy_customers WHERE (id=%s OR phone=%s)", (login_identifier, login_identifier))
        customer = cur.fetchone()
        conn.close()
        
        if customer and check_password_hash(customer[2], password):
            session["role"] = "customer"
            session["customer_id"] = customer[0]
            session["customer_name"] = customer[1]
            return redirect("/customer")
        else:
            flash("Invalid Customer ID/Phone or PIN", "danger")
            return redirect("/")
        
    elif role == "delivery":
        login_identifier = request.form.get("delivery_username")
        password = request.form.get("delivery_password")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, username, password, name FROM delivery_staff WHERE (id=%s OR username=%s)", (login_identifier, login_identifier))
        staff = cur.fetchone()
        conn.close()
        
        if staff and check_password_hash(staff[2], password):
            session["role"] = "delivery"
            session["staff_id"] = staff[0]
            session["staff_name"] = staff[3]
            return redirect("/delivery/dashboard")
        else:
            flash("Invalid Staff Credentials", "danger")
            return redirect("/")
            
    return redirect("/")



@app.route("/forgot_password", methods=["POST"])
def forgot_password():
    user_type = request.form.get("user_type")
    name = request.form.get("name")
    user_id = request.form.get("user_id")
    contact_number = request.form.get("contact_number")
    
    reason = f"Password Recovery Request | Name: {name} | {user_type.capitalize()} ID: {user_id} | Phone: {contact_number}"
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    customer_id = None
    staff_id = None
    
    if user_type == "customer":
        customer_id = user_id
    else:
        staff_id = user_id
        
    try:
        cur.execute("""
            INSERT INTO attendance_requests (customer_id, staff_id, reason, status)
            VALUES (%s, %s, %s, 'Pending')
        """, (customer_id, staff_id, reason))
        conn.commit()
        flash("Ticket raised successfully! Please contact admin for your new password.")
    except Exception as e:
        flash(f"Error raising ticket: {str(e)}")
    finally:
        conn.close()
        
    return redirect("/")

# ------------------ ADMIN DASHBOARD & ACCOUNT ------------------
@app.route("/admin")
def admin_dashboard():
    if session.get("role") != "admin":
        return redirect("/")
    return render_template("admin_dashboard.html")

@app.route("/admin/account", methods=["GET", "POST"])
def admin_account():
    if session.get("role") != "admin":
        return redirect("/")
        
    admin_id = session.get("admin_id")
    
    # If somehow admin_id wasn't set (e.g., legacy session), try to fetch the default admin
    if not admin_id:
        # We assume the default admin has ID 1 if not in session, but safer to query by 'admin' username
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM admin_users WHERE username='admin'")
        res = cur.fetchone()
        if res:
            admin_id = res[0]
            session["admin_id"] = admin_id
        conn.close()

    if not admin_id:
        return "Admin account not found. Please log out and back in."

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        new_name = request.form.get("name")
        new_password = request.form.get("password")
        
        if new_password:
            hashed_password = generate_password_hash(new_password)
            cur.execute("UPDATE admin_users SET name=%s, password=%s WHERE id=%s", (new_name, hashed_password, admin_id))
        else:
            cur.execute("UPDATE admin_users SET name=%s WHERE id=%s", (new_name, admin_id))

        conn.commit()
        session["admin_name"] = new_name
        flash("Account details updated successfully!")
        return redirect("/admin")

    cur.execute("SELECT * FROM admin_users WHERE id=%s", (admin_id,))
    admin = cur.fetchone()
    conn.close()

    return render_template("admin_account.html", admin=admin)

# ------------------ CUSTOMER DASHBOARD ------------------
@app.route("/customer")
def customer_dashboard():
    if session.get("role") != "customer":
        return redirect("/")
    return render_template("customer_dashboard.html")

# ------------------ SNACKS MENU ------------------
# ============================================================
# SNACKS MODULE — Full Supermarket Billing System
# ============================================================

def generate_barcode_base64(data: str) -> str:
    """Generate a Code128 barcode and return as base64 PNG string."""
    CODE128 = barcode.get_barcode_class('code128')
    # If it's a snack ID, we might want to just encode the ID number or the SNACK:ID format
    # For compatibility with existing scanner logic, we can keep the SNACK: format
    # but barcodes are often used just for IDs. Let's keep the prefix for now.
    bar = CODE128(data, writer=ImageWriter())
    buf = io.BytesIO()
    bar.write(buf)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


@app.route("/snacks", methods=["GET", "POST"])
def snacks_menu():
    if session.get("role") != "admin":
        return redirect("/")

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add":
            name = request.form["name"]
            purchase_price = float(request.form.get("purchase_price", 0))
            retail_price = float(request.form.get("retail_price", 0))
            wholesale_price = float(request.form.get("wholesale_price", 0))
            stock = int(request.form.get("stock", 0))
            
            file = request.files.get("image")
            image_url = None
            if file and file.filename != '':
                if CLOUDINARY_URL:
                    try:
                        print("DEBUG: Attempting Cloudinary upload...")
                        upload_result = cloudinary.uploader.upload(file, folder="snacks")
                        image_url = upload_result.get("secure_url")
                        print(f"DEBUG: Cloudinary upload successful: {image_url}")
                    except Exception as e:
                        print(f"ERROR: Cloudinary upload failed: {e}")
                        flash(f"Image upload failed: {str(e)}", "danger")
                else:
                    flash("Cloudinary is not configured. Please contact admin.", "warning")

            cur.execute(
                "INSERT INTO snacks_menu (name, price, purchase_price, retail_price, wholesale_price, stock, image_url) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (name, retail_price, purchase_price, retail_price, wholesale_price, stock, image_url)
            )
            conn.commit()
            flash(f"Product '{name}' added successfully!", "success")

        elif action == "edit":
            item_id = request.form["item_id"]
            name = request.form["name"]
            purchase_price = float(request.form.get("purchase_price", 0))
            retail_price = float(request.form.get("retail_price", 0))
            wholesale_price = float(request.form.get("wholesale_price", 0))
            stock = int(request.form.get("stock", 0))

            file = request.files.get("image")
            if file and file.filename != '':
                new_image_url = None
                if CLOUDINARY_URL:
                    try:
                        upload_result = cloudinary.uploader.upload(file, folder="snacks")
                        new_image_url = upload_result.get("secure_url")
                    except Exception as e:
                        print(f"Cloudinary upload failed: {e}")
                        flash(f"Image update failed: {str(e)}", "danger")
                else:
                    flash("Cloudinary is not configured. Product updated without image.", "warning")
                
                if new_image_url:
                    cur.execute(
                        "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s, image_url=%s WHERE id=%s",
                        (name, retail_price, purchase_price, retail_price, wholesale_price, stock, new_image_url, item_id)
                    )
                else:
                    cur.execute(
                        "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s WHERE id=%s",
                        (name, retail_price, purchase_price, retail_price, wholesale_price, stock, item_id)
                    )
            else:
                cur.execute(
                    "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s WHERE id=%s",
                    (name, retail_price, purchase_price, retail_price, wholesale_price, stock, item_id)
                )
            conn.commit()
            flash("Product updated.", "success")

        elif action == "delete":
            item_id = request.form["item_id"]
            cur.execute("DELETE FROM snacks_menu WHERE id=%s", (item_id,))
            conn.commit()
            flash("Product deleted.", "info")

        return redirect("/snacks")

    cur.execute("SELECT id, name, purchase_price, retail_price, wholesale_price, stock, image_url FROM snacks_menu ORDER BY id ASC")
    items = cur.fetchall()
    conn.close()
    return render_template("snacks/snacks_menu.html", items=items)


@app.route("/snacks/barcode/<int:item_id>")
def snacks_barcode(item_id):
    """Return Barcode PDF for a product (Code128)."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM snacks_menu WHERE id=%s", (item_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return "Not found", 404
    
    # 1. Generate Barcode Image in memory
    barcode_data = f"SNACK:{row[0]}"
    CODE128 = barcode.get_barcode_class('code128')
    bar = CODE128(barcode_data, writer=ImageWriter())
    
    img_buf = io.BytesIO()
    # Don't write text on the image itself; we'll add it in the PDF for better control
    bar.write(img_buf, options={'write_text': False, 'module_height': 15.0})
    img_buf.seek(0)
    
    # 2. Generate PDF with ReportLab
    from reportlab.lib.pagesizes import portrait
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    
    pdf_buf = io.BytesIO()
    # Label size: 60mm x 40mm
    label_size = (60*mm, 40*mm)
    doc = SimpleDocTemplate(pdf_buf, pagesize=label_size, topMargin=5*mm, bottomMargin=2*mm, leftMargin=5*mm, rightMargin=5*mm)
    
    elements = []
    
    # Add Barcode Image
    img = Image(img_buf, width=50*mm, height=20*mm)
    elements.append(img)
    
    # Add Product Name below with small font
    styles = getSampleStyleSheet()
    small_style = ParagraphStyle('SmallStyle', parent=styles['Normal'], fontSize=7, alignment=1, leading=8)
    
    elements.append(Spacer(1, 2*mm))
    elements.append(Paragraph(f"<b>{row[1]} [ID:{row[0]}]</b>", small_style))
    
    doc.build(elements)
    pdf_buf.seek(0)
    
    return send_file(
        pdf_buf, 
        mimetype="application/pdf", 
        as_attachment=True, 
        download_name=f"Barcode_{row[1].replace(' ', '_')}.pdf"
    )


@app.route("/api/snacks/products", methods=["GET", "POST", "PUT", "DELETE"])
def api_snacks_products():
    """REST API for snacks product management."""
    if session.get("role") != "admin":
        return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "GET":
        from psycopg2.extras import RealDictCursor
        cur.close()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, name, purchase_price, retail_price, wholesale_price, stock, image_url FROM snacks_menu ORDER BY id ASC")
        items = cur.fetchall()
        conn.close()
        return jsonify(items)

    elif request.method == "POST":
        name = request.form.get("name")
        purchase_price = float(request.form.get("purchase_price", 0))
        retail_price = float(request.form.get("retail_price", 0))
        wholesale_price = float(request.form.get("wholesale_price", 0))
        stock = int(request.form.get("stock", 0))
        
        file = request.files.get("image")
        image_url = None
        if file and file.filename != '':
            # Validate max size (2MB) before saving
            file.seek(0, 2)
            size = file.tell()
            file.seek(0)
            if size > 2 * 1024 * 1024:
                return jsonify({"error": "Image size exceeds 2MB limit!"}), 400
            
            if CLOUDINARY_URL:
                try:
                    print("DEBUG API: Attempting Cloudinary upload...")
                    upload_result = cloudinary.uploader.upload(file, folder="snacks")
                    image_url = upload_result.get("secure_url")
                    print(f"DEBUG API: Cloudinary upload successful: {image_url}")
                except Exception as e:
                    print(f"ERROR API: Cloudinary upload failed: {e}")
                    return jsonify({"error": f"Cloudinary upload failed: {str(e)}"}), 500
            else:
                return jsonify({"error": "Cloudinary is not configured on the server."}), 400
        
        from psycopg2.extras import RealDictCursor
        cur.close()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "INSERT INTO snacks_menu (name, price, purchase_price, retail_price, wholesale_price, stock, image_url) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id, image_url",
            (name, retail_price, purchase_price, retail_price, wholesale_price, stock, image_url)
        )
        res = cur.fetchone()
        conn.commit()
        conn.close()
        return jsonify({"success": True, "id": res['id'], "image_url": res['image_url']})

    elif request.method == "PUT":
        item_id = request.form.get("id")
        name = request.form.get("name")
        purchase_price = float(request.form.get("purchase_price", 0))
        retail_price = float(request.form.get("retail_price", 0))
        wholesale_price = float(request.form.get("wholesale_price", 0))
        stock = int(request.form.get("stock", 0))

        file = request.files.get("image")
        if file and file.filename != '':
            # Validate max size
            file.seek(0, 2)
            size = file.tell()
            file.seek(0)
            if size > 2 * 1024 * 1024:
                return jsonify({"error": "Image size exceeds 2MB limit!"}), 400

            new_image_url = None
            if CLOUDINARY_URL:
                try:
                    print("DEBUG API PUT: Attempting Cloudinary upload...")
                    upload_result = cloudinary.uploader.upload(file, folder="snacks")
                    new_image_url = upload_result.get("secure_url")
                    print(f"DEBUG API PUT: Cloudinary upload successful: {new_image_url}")
                except Exception as e:
                    print(f"ERROR API PUT: Cloudinary upload failed: {e}")
                    return jsonify({"error": f"Cloudinary update failed: {str(e)}"}), 500
            else:
                return jsonify({"error": "Cloudinary is not configured."}), 400

            if new_image_url:
                cur.execute(
                    "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s, image_url=%s WHERE id=%s",
                    (name, retail_price, purchase_price, retail_price, wholesale_price, stock, new_image_url, item_id)
                )
            else:
                cur.execute(
                    "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s WHERE id=%s",
                    (name, retail_price, purchase_price, retail_price, wholesale_price, stock, item_id)
                )
        else:
            cur.execute(
                "UPDATE snacks_menu SET name=%s, price=%s, purchase_price=%s, retail_price=%s, wholesale_price=%s, stock=%s WHERE id=%s",
                (name, retail_price, purchase_price, retail_price, wholesale_price, stock, item_id)
            )
        conn.commit()
        conn.close()
        return jsonify({"success": True})

    elif request.method == "DELETE":
        item_id = request.args.get("id")
        if not item_id:
            return jsonify({"error": "Missing ID"}), 400
        cur.execute("DELETE FROM snacks_menu WHERE id=%s", (item_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/snacks/product_lookup")
def snacks_product_lookup():
    """Lookup product by ID or name for billing/autocomplete."""
    q = request.args.get("q", "").strip()
    conn = get_db_connection()
    cur = conn.cursor()
    if q.isdigit():
        cur.execute("SELECT id, name, retail_price, wholesale_price, stock FROM snacks_menu WHERE id=%s", (int(q),))
        row = cur.fetchone()
        conn.close()
        if row:
            return jsonify({"found": True, "id": row[0], "name": row[1], "retail_price": row[2], "wholesale_price": row[3], "stock": row[4]})
        return jsonify({"found": False})
    else:
        cur.execute("SELECT id, name, retail_price, wholesale_price, stock FROM snacks_menu WHERE name LIKE %s ORDER BY name LIMIT 10", (f"{q}%",))
        rows = cur.fetchall()
        conn.close()
        return jsonify([{"id": r[0], "name": r[1], "retail_price": r[2], "wholesale_price": r[3], "stock": r[4]} for r in rows])


@app.route("/snacks/billing", methods=["GET", "POST"])
def snacks_billing():
    if session.get("role") != "admin":
        return redirect("/")

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        # Checkout: receive JSON cart
        data = request.get_json()
        if data:
            mode = data.get("mode", "retail")
            cart = data.get("cart", [])
            customer_name = data.get("customer_name", "")
            payment_mode = data.get("payment_mode", "Cash")
            discount = float(data.get("discount", 0))

            subtotal = 0
            for item in cart:
                subtotal += item["unit_price"] * item["qty"]
            grand_total = max(0.0, subtotal - discount)

            pay_status = "Pending" if payment_mode == "Credit" else "Paid"
            
            cur.execute(
                "INSERT INTO snacks_bills (bill_mode, subtotal, discount, grand_total, customer_name, payment_mode, payment_status) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (mode, subtotal, discount, grand_total, customer_name, payment_mode, pay_status)
            )
            bill_id = cur.fetchone()[0]

            for item in cart:
                qty_to_deduct = item["qty"]
                total_cost_for_item = 0
                
                # FIFO Batch Logic
                cur.execute("SELECT id, remaining_qty, purchase_price FROM snacks_stock_in WHERE item_id=%s AND remaining_qty > 0 ORDER BY id ASC", (item["id"],))
                batches = cur.fetchall()
                
                for bid, bremain, bcost in batches:
                    if qty_to_deduct <= 0: break
                    
                    if bremain >= qty_to_deduct:
                        cur.execute("UPDATE snacks_stock_in SET remaining_qty = remaining_qty - %s WHERE id=%s", (qty_to_deduct, bid))
                        total_cost_for_item += qty_to_deduct * bcost
                        qty_to_deduct = 0
                    else:
                        cur.execute("UPDATE snacks_stock_in SET remaining_qty = 0 WHERE id=%s", (bid,))
                        total_cost_for_item += bremain * bcost
                        qty_to_deduct -= bremain
                
                # If there's still quantity left but no batches, use the menu's default purchase price
                if qty_to_deduct > 0:
                    cur.execute("SELECT purchase_price FROM snacks_menu WHERE id=%s", (item["id"],))
                    default_cost = cur.fetchone()[0] or 0
                    total_cost_for_item += qty_to_deduct * default_cost
                
                unit_cost = total_cost_for_item / item["qty"]

                cur.execute(
                    "INSERT INTO snacks_bill_items (bill_id, item_id, item_name, qty, unit_price, cost_price, total) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (bill_id, item["id"], item["name"], item["qty"], item["unit_price"], unit_cost, item["unit_price"] * item["qty"])
                )
                # Deduct total stock in snacks_menu
                cur.execute("UPDATE snacks_menu SET stock = stock - %s WHERE id=%s", (item["qty"], item["id"]))
                # Log to sales for history
                cur.execute("INSERT INTO snacks_sales (item_id, qty, total) VALUES (%s, %s, %s)",
                            (item["id"], item["qty"], item["unit_price"] * item["qty"]))

            conn.commit()
            conn.close()
            return jsonify({"success": True, "bill_id": bill_id, "grand_total": grand_total})

        conn.close()
        return jsonify({"success": False, "error": "No data"})

    cur.execute("SELECT id, name, retail_price, wholesale_price, stock FROM snacks_menu WHERE stock > 0 ORDER BY name")
    items = cur.fetchall()
    conn.close()
    return render_template("snacks/snacks_billing.html", items=items)


@app.route("/snacks/mark_paid", methods=["POST"])
def snacks_mark_paid():
    if session.get("role") != "admin":
        return redirect("/")
    bill_id = request.form.get("bill_id")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE snacks_bills SET payment_status = 'Paid' WHERE id = %s", (bill_id,))
    conn.commit()
    conn.close()
    flash("Payment marked as PAID.", "success")
    return redirect(request.referrer or "/snacks/accounts")


@app.route("/snacks/bill/<int:bill_id>")
def snacks_bill_detail(bill_id):
    if session.get("role") != "admin":
        return redirect("/")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM snacks_bills WHERE id=%s", (bill_id,))
    bill_raw = cur.fetchone()
    if not bill_raw:
        return "Bill not found", 404
        
    bill = list(bill_raw)
    if hasattr(bill[8], 'strftime'):
        bill[8] = bill[8].strftime('%Y-%m-%d %H:%M:%S')
    elif bill[8]:
        bill[8] = str(bill[8])
        
    cur.execute("SELECT * FROM snacks_bill_items WHERE bill_id=%s", (bill_id,))
    items = cur.fetchall()
    conn.close()
    return render_template("snacks/bill_receipt.html", bill=bill, items=items)


@app.route("/snacks/stock", methods=["GET", "POST"])
def snacks_stock():
    if session.get("role") != "admin":
        return redirect("/")
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        item_id = int(request.form["item_id"])
        qty = int(request.form["qty"])
        purchase_price = float(request.form.get("purchase_price", 0))
        supplier = request.form.get("supplier", "")
        notes = request.form.get("notes", "")
        cur.execute(
            "INSERT INTO snacks_stock_in (item_id, qty, remaining_qty, purchase_price, supplier, notes) VALUES (%s, %s, %s, %s, %s, %s)",
            (item_id, qty, qty, purchase_price, supplier, notes)
        )
        cur.execute("UPDATE snacks_menu SET stock = stock + %s WHERE id=%s", (qty, item_id))
        # Update purchase price if provided
        if purchase_price > 0:
            cur.execute("UPDATE snacks_menu SET purchase_price=%s WHERE id=%s", (purchase_price, item_id))
        conn.commit()
        flash(f"Stock received: {qty} units added.", "success")
        return redirect("/snacks/stock")

    cur.execute("SELECT id, name, stock, purchase_price, retail_price FROM snacks_menu ORDER BY name")
    items = cur.fetchall()
    cur.execute("""
        SELECT si.id, m.name, si.qty, si.purchase_price, si.supplier, si.date, si.notes
        FROM snacks_stock_in si
        JOIN snacks_menu m ON si.item_id = m.id
        ORDER BY si.date DESC LIMIT 50
    """)
    stock_log = cur.fetchall()
    conn.close()
    return render_template("snacks/snacks_stock.html", items=items, stock_log=stock_log)


@app.route("/snacks/inventory")
def snacks_inventory():
    if session.get("role") != "admin":
        return redirect("/")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, purchase_price, retail_price, wholesale_price, stock FROM snacks_menu ORDER BY name ASC")
    items = cur.fetchall()
    conn.close()
    return render_template("snacks/snacks_inventory.html", items=items)


@app.route("/snacks/accounts")
def snacks_accounts():
    if session.get("role") != "admin":
        return redirect("/")
    
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    
    conn = get_db_connection()
    cur = conn.cursor()

    # Base queries for summaries
    where_clause = ""
    params = []
    if start_date and end_date:
        where_clause = " WHERE date BETWEEN %s AND %s"
        params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]
    elif start_date:
        where_clause = " WHERE date >= %s"
        params = [f"{start_date} 00:00:00"]
    elif end_date:
        where_clause = " WHERE date <= %s"
        params = [f"{end_date} 23:59:59"]

    # Total revenue by mode (Retail/Wholesale) - Filtered
    cur.execute(f"SELECT bill_mode, COUNT(*), SUM(grand_total) FROM snacks_bills {where_clause} GROUP BY bill_mode", params)
    mode_summary = cur.fetchall()
    
    # NEW: Total revenue by payment mode (Cash/Online) - Filtered
    cur.execute(f"SELECT payment_mode, SUM(grand_total) FROM snacks_bills {where_clause} GROUP BY payment_mode", params)
    payment_summary = cur.fetchall()
    
    cash_total = 0.0
    online_total = 0.0
    for p_mode, p_total in payment_summary:
        val = float(p_total or 0.0)
        pm_lower = (p_mode or "").lower()
        if 'cash' in pm_lower: 
            cash_total = float(cash_total) + val
        else:
            online_total = float(online_total) + val

    # Date range heading for cards
    period_label = "Filtered Period" if (start_date or end_date) else "Total (All Time)"

    # Today's sales (Always today)
    today = datetime.date.today().isoformat()
    cur.execute("SELECT COUNT(*), SUM(grand_total) FROM snacks_bills WHERE date::TEXT LIKE %s", (f"{today}%",))
    today_row = cur.fetchone()
    today_bills = today_row[0] or 0
    today_revenue = today_row[1] or 0
    
    # Today's Cash vs Online
    cur.execute("SELECT payment_mode, SUM(grand_total) FROM snacks_bills WHERE date::TEXT LIKE %s GROUP BY payment_mode", (f"{today}%",))
    today_payment_rows = cur.fetchall()
    today_cash = 0.0
    today_online = 0.0
    for p_mode, p_total in today_payment_rows:
        val = float(p_total or 0.0)
        pm_lower = (p_mode or "").lower()
        if 'cash' in pm_lower:
            today_cash = float(today_cash) + val
        else:
            today_online = float(today_online) + val

    # Profit calculation for the SELECTED RANGE using recorded cost_price (FIFO)
    # COALESCE handles historical bills where cost_price might be 0
    if where_clause:
        cur.execute(f"""
            SELECT 
                SUM(bi.qty * (bi.unit_price - (CASE WHEN bi.cost_price > 0 THEN bi.cost_price ELSE m.purchase_price END))) as profit,
                SUM(bi.total) as revenue
            FROM snacks_bill_items bi
            JOIN snacks_menu m ON bi.item_id = m.id
            JOIN snacks_bills b ON bi.bill_id = b.id
            {where_clause.replace('date', 'b.date')}
        """, params)
    else:
        cur.execute("""
            SELECT 
                SUM(bi.qty * (bi.unit_price - (CASE WHEN bi.cost_price > 0 THEN bi.cost_price ELSE m.purchase_price END))) as profit,
                SUM(bi.total) as revenue
            FROM snacks_bill_items bi
            JOIN snacks_menu m ON bi.item_id = m.id
        """)
        
    profit_row = cur.fetchone()
    total_profit = profit_row[0] or 0
    total_revenue = profit_row[1] or 0

    # Recent bills - Filtered
    recent_query = f"SELECT id, date, bill_mode, customer_name, payment_mode, grand_total, payment_status FROM snacks_bills {where_clause} ORDER BY id DESC LIMIT 50"
    cur.execute(recent_query, params)
    raw_recent = cur.fetchall()
    
    recent_bills = []
    for rb in raw_recent:
        rb_list = list(rb)
        if hasattr(rb_list[1], 'strftime'):
            rb_list[1] = rb_list[1].strftime('%Y-%m-%d %H:%M:%S')
        elif rb_list[1]:
            rb_list[1] = str(rb_list[1])
        recent_bills.append(rb_list)

    # Top selling products - Filtered
    if where_clause:
        cur.execute(f"""
            SELECT bi.item_name, SUM(bi.qty) as total_qty, SUM(bi.total) as total_revenue
            FROM snacks_bill_items bi
            JOIN snacks_bills b ON bi.bill_id = b.id
            {where_clause.replace('date', 'b.date')}
            GROUP BY bi.item_name
            ORDER BY total_qty DESC LIMIT 10
        """, params)
    else:
        cur.execute("""
            SELECT bi.item_name, SUM(bi.qty) as total_qty, SUM(bi.total) as total_revenue
            FROM snacks_bill_items bi
            GROUP BY bi.item_name
            ORDER BY total_qty DESC LIMIT 10
        """)
    top_products = cur.fetchall()

    conn.close()
    return render_template("snacks/snacks_accounts.html",
        mode_summary=mode_summary,
        today_bills=today_bills,
        today_revenue=today_revenue,
        today_cash=today_cash,
        today_online=today_online,
        cash_total=cash_total,
        online_total=online_total,
        total_profit=total_profit,
        total_revenue=total_revenue,
        recent_bills=recent_bills,
        top_products=top_products,
        start_date=start_date,
        end_date=end_date,
        period_label=period_label
    )

@app.route("/snacks/generate_summary")
def generate_snacks_summary():
    if session.get("role") != "admin":
        return redirect("/")
    
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    today = datetime.date.today()
    this_month = today.strftime("%Y-%m")
    
    # Filtering logic for report
    report_where = ""
    report_params = []
    
    if start_date and end_date:
        report_where = " WHERE date BETWEEN %s AND %s"
        report_params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]
        period_title = f"{start_date} to {end_date}"
    elif start_date:
        report_where = " WHERE date >= %s"
        report_params = [f"{start_date} 00:00:00"]
        period_title = f"Since {start_date}"
    elif end_date:
        report_where = " WHERE date <= %s"
        report_params = [f"{end_date} 23:59:59"]
        period_title = f"Up to {end_date}"
    else:
        report_where = " WHERE date::TEXT LIKE %s"
        report_params = [f"{this_month}%"]
        period_title = today.strftime("%B %Y")

    # Filtered Period stats
    cur.execute(f"SELECT COUNT(*), SUM(grand_total) FROM snacks_bills {report_where}", report_params)
    p_row = cur.fetchone()
    p_count = p_row[0] or 0
    p_rev = p_row[1] or 0
    
    # Filtered Period Profit
    cur.execute(f"""
        SELECT SUM(bi.qty * (bi.unit_price - m.purchase_price))
        FROM snacks_bill_items bi
        JOIN snacks_menu m ON bi.item_id = m.id
        JOIN snacks_bills b ON bi.bill_id = b.id
        {report_where.replace('date', 'b.date')}
    """, report_params)
    p_profit = cur.fetchone()[0] or 0
    
    # Daily breakdown for report
    cur.execute(f"""
        SELECT d, bills, revenue, 
               (SELECT SUM(bi.qty * (bi.unit_price - m.purchase_price)) 
                FROM snacks_bill_items bi 
                JOIN snacks_menu m ON bi.item_id = m.id 
                JOIN snacks_bills b2 ON bi.bill_id = b2.id 
                WHERE to_char(b2.date, 'YYYY-MM-DD') = d) as profit
        FROM (
            SELECT to_char(date, 'YYYY-MM-DD') as d, COUNT(*) as bills, SUM(grand_total) as revenue
            FROM snacks_bills 
            {report_where}
            GROUP BY d
        ) ORDER BY d DESC
    """, report_params)
    daily_sales = cur.fetchall()
    
    conn.close()
    
    # PDF Setup
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    
    # Filename shows range if exists
    fname_part = f"{start_date}_to_{end_date}" if (start_date and end_date) else this_month
    filename = f"snacks_summary_{fname_part}.pdf"
    filepath = os.path.join("static", "reports", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    doc = SimpleDocTemplate(filepath, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], alignment=1, fontSize=20, spaceAfter=20)
    subtitle_style = ParagraphStyle('Sub', parent=styles['Normal'], alignment=1, fontSize=12, spaceAfter=20)
    section_style = ParagraphStyle('Sec', parent=styles['Normal'], fontSize=14, spaceBefore=15, spaceAfter=10, fontName='Helvetica-Bold')

    elements.append(Paragraph("HARI SAKTHI DAIRY & SNACKS", title_style))
    elements.append(Paragraph(f"Sales Summary Report: {period_title}", subtitle_style))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.black))
    elements.append(Spacer(1, 0.3*inch))
    
    # Highlights Table
    h_data = [
        ["Period Summary", "Bills", "Revenue", "Est. Profit"],
        [period_title, str(p_count), f"Rs.{p_rev:.2f}", f"Rs.{p_profit:.2f}"]
    ]
    ht = Table(h_data, colWidths=[2.2*inch, 1.2*inch, 1.5*inch, 1.5*inch])
    ht.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 1, colors.grey),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (1,0), (-1,-1), 'CENTER'),
        ('PADDING', (0,0), (-1,-1), 10),
    ]))
    elements.append(Paragraph("Financial Highlights", section_style))
    elements.append(ht)
    elements.append(Spacer(1, 0.3*inch))
    
    # Daily Sales Table
    elements.append(Paragraph("Daily Sales Breakdown", section_style))
    if not daily_sales:
        elements.append(Paragraph("No sales records found for this period.", styles['Italic']))
    else:
        d_data = [["Date", "Bills", "Revenue", "Est. Profit"]]
        for row in daily_sales:
            d_data.append([row[0], str(row[1]), f"Rs.{row[2]:.2f}", f"Rs.{(row[3] or 0):.2f}"])
        
        dt = Table(d_data, colWidths=[1.8*inch, 1.2*inch, 1.7*inch, 1.7*inch])
        dt.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
            ('GRID', (0,0), (-1,-1), 0.5, colors.silver),
            ('ALIGN', (1,0), (-1,-1), 'CENTER'),
            ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ]))
        elements.append(dt)
    
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph("Report generated on " + today.strftime("%d-%m-%Y %H:%M"), styles['Italic']))
    
    doc.build(elements)
    return send_file(filepath, as_attachment=True)

# ------------------ DAIRY MODULE ------------------

@app.route("/dairy")
def dairy_menu():
    if session.get("role") != "admin":
        return redirect("/")
    return render_template("dairy/dairy_menu.html")

@app.route("/dairy/attendance", methods=["GET", "POST"])
def dairy_attendance():
    if session.get("role") != "admin":
        return redirect("/")
    
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        if "customer_name" in request.form:
            cur.execute("INSERT INTO dairy_customers (name, phone) VALUES (%s, %s)", 
                       (request.form["customer_name"], request.form.get("customer_phone", "")))
            conn.commit()
        elif "log_customer_id" in request.form:
             # Fix: Use provided date or fallback to today
             log_date = request.form.get("log_date", datetime.date.today().isoformat())
             cust_id = request.form["log_customer_id"]
             qty = request.form["quantity"]
             
             cur.execute("SELECT id FROM dairy_logs WHERE customer_id=%s AND date=%s", (cust_id, log_date))
             existing = cur.fetchone()
             if existing:
                 cur.execute("UPDATE dairy_logs SET quantity=%s, time_slot='AM' WHERE id=%s", (qty, existing[0]))
             else:
                 cur.execute("INSERT INTO dairy_logs (customer_id, date, time_slot, quantity) VALUES (%s, %s, 'AM', %s)",
                            (cust_id, log_date, qty))
             conn.commit()

    # Sort customers by Name for the dropdown
    cur.execute("SELECT * FROM dairy_customers ORDER BY name ASC")
    customers = cur.fetchall()
    
    # Fix: Use Python date for today's logs query
    today_date = datetime.date.today()
    cur.execute("""
        SELECT l.id, c.name, l.quantity, l.date 
        FROM dairy_logs l 
        JOIN dairy_customers c ON l.customer_id = c.id 
        WHERE l.date = %s
    """, (today_date,))
    todays_logs = cur.fetchall()

    conn.close()
    return render_template("dairy/dairy_attendance.html", customers=customers, logs=todays_logs)

@app.route("/dairy/billing")
def dairy_billing():
    if session.get("role") != "admin":
        return redirect("/")
    
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch customer details, total MTD quantity, and actual calculated bill data
    cur.execute("""
        SELECT c.id, c.name, 
               COALESCE((SELECT SUM(quantity) FROM dairy_logs l WHERE l.customer_id = c.id AND to_char(l.date, 'YYYY-MM') = %s), 0) as total_liters, 
               c.net_payable,
               c.last_bill_generated_on
        FROM dairy_customers c
        ORDER BY c.delivery_order ASC, c.id ASC
    """, (month_str,))
    billing_data = cur.fetchall()
    
    final_data = []
    for row in billing_data:
        cid = row[0]
        cname = row[1]
        liters = row[2]
        net_payable = row[3]
        last_gen = row[4]
        
        pdf_filename = None
        if last_gen:
            s_name = cname.replace(" ", "_").replace("/", "-").replace("\\", "-")
            pdf_filename = f"{s_name}_{month_str}_bill.pdf"
            
        final_data.append((cid, cname, liters, net_payable, last_gen, pdf_filename))
        
    conn.close()
    
    return render_template("dairy/dairy_billing.html", billing_data=final_data, month_str=month_str)

@app.route("/dairy/customer")
def dairy_customer_view():
    if session.get("role") != "customer":
        return redirect("/")
    
    customer_id = session.get("customer_id")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT l.id, l.customer_id, l.date, l.time_slot, l.quantity, p.product_name 
        FROM dairy_logs l 
        LEFT JOIN customer_products p ON l.product_id = p.id 
        WHERE l.customer_id = %s 
        ORDER BY l.date DESC LIMIT 100
    """, (customer_id,))
    my_logs = cur.fetchall()

    # Fetch request history
    cur.execute("""
        SELECT r.id, r.new_date, r.new_time_slot, r.new_quantity, r.reason, r.status, r.admin_response, r.product_name 
        FROM attendance_requests r 
        WHERE r.customer_id = %s 
        ORDER BY r.id DESC LIMIT 20
    """, (customer_id,))
    my_requests = cur.fetchall()
    
    # 3. Fetch Master Products for Request Modal
    cur.execute("SELECT name FROM dairy_master_products ORDER BY id ASC")
    master_products = [row[0] for row in cur.fetchall()]
    
    conn.close()

    return render_template("dairy/dairy_customer.html", logs=my_logs, requests=my_requests, master_products=master_products)

@app.route("/dairy/accounts")
def dairy_accounts_overview():
    if session.get("role") != "admin":
        return redirect("/")
    
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Fetch all staff
    cur.execute("SELECT id, name FROM delivery_staff")
    staff_list = cur.fetchall()
    
    # Add an entry for unassigned
    staff_data = [] # List of {staff_info, customers: [], totals: {bill, paid_cash, paid_online, balance}}
    
    # Process each staff (including None for unassigned)
    all_staff_ids = [s[0] for s in staff_list] + [None]
    staff_names = {s[0]: s[1] for s in staff_list}
    staff_names[None] = "Unassigned"
    
    grand_totals = {"bill": 0.0, "cash": 0.0, "online": 0.0, "balance": 0.0}

    for sid in all_staff_ids:
        if sid is None:
            cur.execute("SELECT id, name, service_charge, last_bill_generated_on, net_payable, billing_type, last_bill_amount FROM dairy_customers WHERE delivery_staff_id IS NULL")
        else:
            cur.execute("SELECT id, name, service_charge, last_bill_generated_on, net_payable, billing_type, last_bill_amount FROM dairy_customers WHERE delivery_staff_id = %s", (sid,))
        
        customers = cur.fetchall()
        if not customers and sid is not None:
            continue
        if not customers and sid is None:
            continue

        staff_entry = {
            "id": sid,
            "name": staff_names[sid],
            "customers": [],
            "totals": {"bill": 0.0, "arrears": 0.0, "cash": 0.0, "online": 0.0, "balance": 0.0}
        }
        
        for cid, cname, service_charge, last_gen, net_payable, billing_type, last_bill_amount in customers:
            # Billing cycle label & description
            btype = billing_type or 'current_month'
            try:
                year, month = map(int, month_str.split("-"))
            except:
                year, month = datetime.date.today().year, datetime.date.today().month

            if btype == 'reservation':
                cycle_label = 'R'
                cycle_start = datetime.date(year, month, 16)
                next_m = month + 1 if month < 12 else 1
                next_y = year if month < 12 else year + 1
                cycle_end = datetime.date(next_y, next_m, 15)
            elif btype == 'month_end':
                cycle_label = 'M'
                bill_month_start = datetime.date(year, month, 1)
                prev_last = bill_month_start - datetime.timedelta(days=1)
                cycle_start = prev_last.replace(day=1)
                cycle_end = prev_last
            else:
                cycle_label = 'CM'
                cycle_start = datetime.date(year, month, 1)
                cycle_end = datetime.date(year, month, calendar.monthrange(year, month)[1])

            billing_cycle_desc = f"{cycle_start.strftime('%d-%b-%Y')} → {cycle_end.strftime('%d-%b-%Y')}"

            # last_gen relevant only if it belongs to the selected month
            last_gen_in_month = None
            if last_gen:
                try:
                    lg_date = datetime.date.fromisoformat(last_gen)
                    # For reservation, the cycle spans two calendar months; check within cycle
                    if cycle_start <= lg_date <= cycle_end:
                        last_gen_in_month = last_gen
                    elif lg_date.strftime('%Y-%m') == month_str:
                        last_gen_in_month = last_gen
                except:
                    pass
            if last_gen_in_month:
                current_bill = float(last_bill_amount or 0.0)
            else:
                current_bill = 0.0
            
            # Arrears represent any unpaid balance from previous months
            # net_payable is the running balance in the DB (Total Debt)
            # If current_bill is part of net_payable, arrears = net_payable - current_bill
            arrears = float(net_payable or 0.0) - current_bill
            if arrears < 0: arrears = 0.0

            # Fetch all payment records for the selected month and sum them
            cur.execute("SELECT amount, payment_mode FROM dairy_payments WHERE customer_id = %s AND month = %s", (cid, month_str))
            all_payments = cur.fetchall()
            
            paid_amount = 0.0
            cash_paid = 0.0
            online_paid = 0.0
            modes = set()
            
            for p_amt_raw, p_mode in all_payments:
                p_amt = float(p_amt_raw or 0)
                paid_amount += p_amt
                if p_mode:
                    modes.add(p_mode)
                if p_mode == "Cash":
                    cash_paid += p_amt
                elif p_mode == "Online":
                    online_paid += p_amt
            
            if not all_payments:
                mode = "N/A"
            elif len(modes) > 1:
                mode = "Mixed"
            elif len(modes) == 1:
                mode = list(modes)[0]
            else:
                mode = "N/A"
            
            # The shown balance is the TOTAL debt (arrears + current_bill) minus what was paid this month.
            total_debt = arrears + current_bill
            balance = total_debt - paid_amount
            
            # Fetch breakdown for accounts display
            # 1. Base products
            cur.execute("SELECT product_name, default_qty, price FROM customer_products WHERE customer_id=%s", (cid,))
            p_details = cur.fetchall()
            cust_products_list = []
            fixed_qty_sum = 0
            base_total = 0
            
            # Number of days in the billing cycle for this customer
            # (Simplification: assuming roughly 30 days or using current month)
            num_days_in_month = calendar.monthrange(int(month_str[:4]), int(month_str[5:7]))[1]
            
            for pname, dqty, price in p_details:
                qty_val = float(dqty or 0)
                price_val = float(price or 0)
                amt = qty_val * price_val * num_days_in_month
                cust_products_list.append({"name": pname, "qty": f"{qty_val} x {num_days_in_month}d", "rate": price_val, "bill": amt})
                fixed_qty_sum += qty_val
                base_total += amt

            # 2. Extras (Actual deviations)
            # This is hard to calculate exactly without full bill logic, 
            # so we'll show a summary or just the total net_payable from the DB.
            # But the user wants to see "Extras" in the breakdown.
            extras_list = []
            cur.execute("SELECT product_name, quantity, rate, amount, date FROM dairy_extra_purchases WHERE customer_id=%s AND to_char(date, 'YYYY-MM') = %s", (cid, month_str))
            for epname, eqty, erate, eamt, edate in cur.fetchall():
                # Fix for subscriptable date error (PG returns date objects)
                day_str = edate.strftime("%d") if hasattr(edate, "strftime") else str(edate)[8:]
                extras_list.append({"name": f"EXT: {epname} ({day_str})", "qty": eqty, "rate": erate, "bill": eamt})

            # Potential filename if generated
            p_filename = None
            if last_gen:
                s_name = cname.replace(" ", "_").replace("/", "-").replace("\\", "-")
                p_filename = f"{s_name}_{month_str}_bill.pdf"

            cust_row = {
                "id": cid,
                "name": cname,
                "products": cust_products_list, 
                "extras": extras_list,
                "fixed_qty_sum": fixed_qty_sum,
                "service_charge": service_charge,
                "service_bill": float(service_charge or 0) * fixed_qty_sum,
                "current_bill": current_bill,
                "arrears": arrears,
                "bill": current_bill + arrears, # Total shown in "Total Bill" column if we keep it
                "paid": paid_amount,
                "mode": mode,
                "balance": balance,
                "last_gen": last_gen,           # raw last generation date
                "last_gen_in_month": last_gen_in_month,  # relevant for selected month only
                "billing_type": btype,
                "cycle_label": cycle_label,
                "billing_cycle_desc": billing_cycle_desc,
                "pdf_filename": p_filename
            }
            
            staff_entry["customers"].append(cust_row)
            # Update staff totals
            staff_entry["totals"]["bill"] = float(staff_entry["totals"]["bill"]) + float(current_bill or 0)
            staff_entry["totals"]["arrears"] = float(staff_entry["totals"]["arrears"]) + float(arrears or 0)
            staff_entry["totals"]["cash"] = float(staff_entry["totals"]["cash"]) + float(cash_paid or 0)
            staff_entry["totals"]["online"] = float(staff_entry["totals"]["online"]) + float(online_paid or 0)
            staff_entry["totals"]["balance"] = float(staff_entry["totals"]["balance"]) + float(balance or 0)
            
            # Update grand totals
            grand_totals["bill"] = float(grand_totals["bill"]) + float(current_bill or 0)
            grand_totals["cash"] = float(grand_totals["cash"]) + float(cash_paid or 0)
            grand_totals["online"] = float(grand_totals["online"]) + float(online_paid or 0)
            grand_totals["balance"] = float(grand_totals["balance"]) + float(balance or 0)
            
        staff_data.append(staff_entry)
        
    conn.close()
    
    return render_template("dairy/dairy_accounts.html", 
                         staff_data=staff_data, 
                         month_str=month_str,
                         grand_totals=grand_totals)

# ------------------ DELIVERY MODULE ------------------

@app.route("/delivery/dashboard", methods=["GET", "POST"])
@app.route("/delivery/dashboard", methods=["GET", "POST"])
def delivery_dashboard():
    if session.get("role") != "delivery":
        return redirect("/")
    
    staff_id = session.get("staff_id")
    conn = get_db_connection()
    cur = conn.cursor()
    
    if request.method == "POST":
        today = request.form.get("date", datetime.date.today().isoformat())
        month_str = datetime.datetime.strptime(today, '%Y-%m-%d').strftime('%Y-%m')
        
        saved_present = 0
        saved_absent = 0

        for key, value in request.form.items():
            # Input name format: "product_{pid}_{cid}"
            if key.startswith("product_") and "_" in key:
                parts = key.split("_")
                if len(parts) >= 3:
                    pid = parts[1]
                    cid = parts[2]
                    qty = float(value) if value else 0.0
                    slot = 'AM' 
                    
                    # Logic: 
                    # If qty > 0 -> Insert/Update
                    # If qty == 0 -> Delete (or keep as 0 if we want to track explicit absence%s Usually delete/0 is absent)
                    # Let's follow existing pattern: Update if exists, Insert if not. 
                    # But if 0%s Older logic seemed to count 0 as absent.
                    
                    cur.execute("SELECT id FROM dairy_logs WHERE product_id=%s AND date=%s", (pid, today))
                    existing = cur.fetchone()
                    
                    if qty > 0:
                        if existing:
                            cur.execute("UPDATE dairy_logs SET quantity=%s WHERE id=%s", (qty, existing[0]))
                        else:
                            cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, time_slot, quantity) VALUES (%s, %s, %s, %s, %s)", 
                                       (cid, pid, today, slot, qty))
                        saved_present = saved_present + 1
                    else:
                        # If 0, treat as absent. Should we delete%s
                        # `toggle_attendance` deletes if 0. 
                        # `delivery_dashboard` previously likely kept 0 or updated to 0%s
                        # Previous code: if value (truthy), insert/update. 
                        # If value was empty string, it skipped.
                        # If value was "0", it updated/inserted 0.
                        # Let's stick to: if existing, update to 0. If not existing, insert 0%s or do nothing%s
                        # User's previous code: `if value:` -> so "0" might be skipped if interpreted as false%s No, string "0" is true.
                        # But `qty = float(value)`. 
                        # Let's allow updating to 0.
                        if existing:
                            cur.execute("UPDATE dairy_logs SET quantity=%s WHERE id=%s", (qty, existing[0]))
                        else:
                             # Only insert 0 if explicitly submitting%s 
                             # If form submits "0", we log it as 0 (Absent).
                             cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, time_slot, quantity) VALUES (%s, %s, %s, %s, %s)", 
                                    (cid, pid, today, slot, qty))
                        saved_absent = saved_absent + 1
            
            elif key.startswith("extra_"):
                cust_id = key.split("_")[1]
                notes = value
                cur.execute("SELECT id FROM dairy_extra_notes WHERE customer_id=%s AND month=%s", (cust_id, month_str))
                existing = cur.fetchone()
                if existing:
                    cur.execute("UPDATE dairy_extra_notes SET notes=%s WHERE id=%s", (notes, existing[0]))
                else:
                    cur.execute("INSERT INTO dairy_extra_notes (customer_id, month, notes) VALUES (%s, %s, %s)", (cust_id, month_str, notes))

        conn.commit()
        flash(f"Success! Updated entries.")
        return redirect("/delivery/dashboard")

    # Fetch assigned customers with join for consistency in indexing (total 12 cols before enrichment)
    cur.execute("""
        SELECT c.*, ds.name 
        FROM dairy_customers c
        LEFT JOIN delivery_staff ds ON c.delivery_staff_id = ds.id
        WHERE c.delivery_staff_id=%s 
        ORDER BY c.delivery_order ASC, c.id ASC
    """, (staff_id,))
    customers_raw = cur.fetchall()
    
    # Enrich with products
    customers = []
    for c in customers_raw:
        cur.execute("SELECT id, product_name, default_qty, price FROM customer_products WHERE customer_id=%s ORDER BY delivery_order", (c[0],))
        products = cur.fetchall()
        c_list = list(c)
        c_list.append(products)
        customers.append(c_list)
        
    today = request.args.get("date", datetime.date.today().isoformat())
    month_str = datetime.datetime.strptime(today, '%Y-%m-%d').strftime('%Y-%m')
    
    logs_map = {} # {product_id: quantity} for TODAY
    extra_notes = {}
    monthly_totals = {} # {product_id: {'P':x, 'A':y}}
    
    if customers:
        # Get all product IDs for these customers
        all_pids = []
        for c in customers:
             for p in c[-1]: # Index -1 is the appended products list
                 all_pids.append(p[0])
        
        if all_pids:
            placeholders = ','.join('%s' for _ in all_pids)
            
            # Fetch logs for today for these products
            cur.execute(f"SELECT product_id, quantity FROM dairy_logs WHERE date=%s AND product_id IN ({placeholders})", [today] + all_pids)
            for row in cur.fetchall():
                logs_map[row[0]] = row[1]
                
            # Fetch monthly totals per product
            cur.execute(f"""
                SELECT product_id, 
                       COUNT(CASE WHEN quantity > 0 THEN 1 END) as present,
                       COUNT(CASE WHEN quantity = 0 THEN 1 END) as absent
                FROM dairy_logs 
                WHERE to_char(date, 'YYYY-MM') = %s AND product_id IN ({placeholders})
                GROUP BY product_id
            """, [month_str] + all_pids)
            for row in cur.fetchall():
                pid, p_cnt, a_cnt = row
                monthly_totals[pid] = {'P': p_cnt, 'A': a_cnt}

        # Fetch extra notes (per customer)
        customer_ids = [c[0] for c in customers]
        cust_placeholders = ','.join('%s' for _ in customer_ids)
        if customer_ids:
            cur.execute(f"SELECT customer_id, notes FROM dairy_extra_notes WHERE month=%s AND customer_id IN ({cust_placeholders})", [month_str] + customer_ids)
            for row in cur.fetchall():
                extra_notes[row[0]] = row[1]

    conn.close()
    return render_template("dairy/delivery_dashboard.html", 
                         customers=customers, 
                         logs_map=logs_map, 
                         extra_notes=extra_notes,
                         monthly_totals=monthly_totals,
                         today=today)

# ------------------ ADMIN - STAFF MANAGEMENT ------------------
@app.route("/dairy/staff", methods=["GET", "POST"])
def manage_staff():
    if session.get("role") != "admin":
        return redirect("/")
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            hashed_password = generate_password_hash(request.form.get("password", "1234"))
            try:
                cur.execute("INSERT INTO delivery_staff (name, username, password) VALUES (%s, %s, %s)", 
                           (request.form.get("name"), request.form.get("username"), hashed_password))
                conn.commit()
            except Exception as e:
                flash(f"Error adding staff: {e}", "danger")
                conn.rollback()
        elif action == "edit":
            staff_id = request.form.get("staff_id")
            raw_password = request.form.get("password")
            if raw_password:
                hashed_password = generate_password_hash(raw_password)
                cur.execute("UPDATE delivery_staff SET name=%s, username=%s, password=%s WHERE id=%s", 
                           (request.form.get("name"), request.form.get("username"), hashed_password, staff_id))
            else:
                cur.execute("UPDATE delivery_staff SET name=%s, username=%s WHERE id=%s", 
                           (request.form.get("name"), request.form.get("username"), staff_id))
            conn.commit()
        elif action == "delete":
            staff_id = request.form["staff_id"]
            cur.execute("UPDATE dairy_customers SET delivery_staff_id=NULL WHERE delivery_staff_id=%s", (staff_id,))
            cur.execute("DELETE FROM delivery_staff WHERE id=%s", (staff_id,))
            conn.commit()
        elif action == "assign":
             staff_id = request.form["staff_id"]
             customer_id = request.form["customer_id"]
             cur.execute("UPDATE dairy_customers SET delivery_staff_id=%s WHERE id=%s", (staff_id, customer_id))
             conn.commit()
    
    cur.execute("SELECT * FROM delivery_staff")
    staff_list = cur.fetchall()
    cur.execute("SELECT * FROM dairy_customers ORDER BY delivery_order ASC, id ASC")
    customers = cur.fetchall()
    conn.close()
    
    staff_customers_map = {}
    unassigned_customers = []
    
    for c in customers:
        sid = c[8] # delivery_staff_id
        if sid:
            if sid not in staff_customers_map:
                staff_customers_map[sid] = []
            staff_customers_map[sid].append(c)
        else:
            unassigned_customers.append(c)
            
    return render_template("dairy/manage_staff.html", 
                           staff_list=staff_list, 
                           customers=customers,
                           staff_customers_map=staff_customers_map,
                           unassigned_customers=unassigned_customers)

@app.route("/dairy/staff/payroll/<int:staff_id>")
def staff_payroll_pdf(staff_id):
    if session.get("role") != "admin":
        return redirect("/")
    
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    # 1. Fetch staff details
    cur.execute("SELECT name, username FROM delivery_staff WHERE id=%s", (staff_id,))
    staff = cur.fetchone()
    if not staff:
        conn.close()
        return "Staff not found", 404
    staff_name = staff['name']
    staff_username = staff['username']
    
    # 2. Fetch assigned customers count
    cur.execute("SELECT COUNT(*) FROM dairy_customers WHERE delivery_staff_id=%s", (staff_id,))
    customer_count = cur.fetchone()[0]

    # 3. Fetch payroll entry for this month
    cur.execute("SELECT * FROM staff_payroll WHERE staff_id=%s AND month=%s", (staff_id, month_str))
    payroll = cur.fetchone()
    
    # 4. Fetch assigned customers and their delivery totals for the month (as detailed breakdown)
    cur.execute("""
        SELECT c.id, c.name, SUM(l.quantity) as total_qty
        FROM dairy_customers c
        LEFT JOIN dairy_logs l ON c.id = l.customer_id AND to_char(l.date, 'YYYY-MM') = %s
        WHERE c.delivery_staff_id = %s
        GROUP BY c.id
    """, (month_str, staff_id))
    customers_data = cur.fetchall()
    
    conn.close()
    
    # Defaults if no payroll entry exists
    p_base = payroll['base_salary'] if payroll else 0.0
    p_comm = payroll['commission'] if payroll else 0.0
    p_bonus = payroll['bonus'] if payroll else 0.0
    p_deduct = payroll['deductions'] if payroll else 0.0
    p_net = payroll['total_paid'] if payroll else (p_base + p_comm + p_bonus - p_deduct)
    p_mode = payroll['payment_mode'] if payroll else "N/A"
    p_notes = payroll['notes'] if payroll else ""

    # 5. Generate PDF
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    
    filename = f"Staff_Payroll_{staff_name.replace(' ', '_')}_{month_str}.pdf"
    filepath = os.path.join("static", "reports", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    doc = SimpleDocTemplate(filepath, pagesize=A4, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle('TitleStyle', parent=styles['Heading1'], fontSize=18, alignment=1, spaceAfter=20)
    normal_style = styles['Normal']
    bold_style = ParagraphStyle('BoldStyle', parent=styles['Normal'], fontName='Helvetica-Bold')
    
    elements.append(Paragraph("<b>HARI SAKTHI DAIRY AND SNACKS</b>", title_style))
    elements.append(Paragraph(f"<b>STAFF PAY SLIP - {month_str}</b>", ParagraphStyle('Sub', alignment=1, fontSize=14, spaceAfter=10)))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=colors.black))
    elements.append(Spacer(1, 0.2*inch))
    
    # Staff Details Table
    staff_info = [
        [Paragraph(f"<b>Staff Name:</b> {staff_name}", normal_style), Paragraph(f"<b>Username:</b> @{staff_username}", normal_style)],
        [Paragraph(f"<b>Staff ID:</b> {staff_id}", normal_style), Paragraph(f"<b>Period:</b> {month_str}", normal_style)],
        [Paragraph(f"<b>Total Customers:</b> {customer_count}", normal_style), Paragraph(f"<b>Payment Date:</b> {datetime.date.today().strftime('%d-%m-%Y')}", normal_style)]
    ]
    info_table = Table(staff_info, colWidths=[3*inch, 3*inch])
    info_table.setStyle(TableStyle([
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 0.2*inch))

    # Earnings & Deductions Table
    elements.append(Paragraph("<b>Payroll Summary</b>", ParagraphStyle('Section', fontSize=12, spaceAfter=10, fontName='Helvetica-Bold')))
    payroll_data = [
        ["Description", "Amount (₹)"],
        ["Base Salary", f"{p_base:.2f}"],
        ["Commission", f"{p_comm:.2f}"],
        ["Bonus", f"{p_bonus:.2f}"],
        [Paragraph("<b>Deductions</b>", normal_style), f"- {p_deduct:.2f}"],
        [Paragraph("<b>NET AMOUNT PAID</b>", bold_style), f"₹ {p_net:.2f}"]
    ]
    pt = Table(payroll_data, colWidths=[4*inch, 2*inch])
    pt.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('FONTNAME', (0,-1), (-1,-1), 'Helvetica-Bold'),
        ('TOPPADDING', (0,-1), (-1,-1), 10),
        ('BOTTOMPADDING', (0,-1), (-1,-1), 10),
    ]))
    elements.append(pt)
    
    if p_notes:
        elements.append(Spacer(1, 0.1*inch))
        elements.append(Paragraph(f"<b>Notes:</b> {p_notes}", normal_style))
    
    elements.append(Spacer(1, 0.3*inch))
    
    # Detailed Breakdown Table
    elements.append(Paragraph("<b>Detailed Customer Delivery Breakdown</b>", ParagraphStyle('Section', fontSize=12, spaceAfter=10, fontName='Helvetica-Bold')))
    detail_header = [["Customer Name", "Total Packets"]]
    total_qty = 0
    for row in customers_data:
        q = float(row['total_qty'] or 0)
        detail_header.append([row['name'], f"{q:.2f}"])
        total_qty += q
    
    detail_header.append([Paragraph("<b>GRAND TOTAL DELIVERED</b>", bold_style), Paragraph(f"<b>{total_qty:.2f}</b>", bold_style)])
    
    dt = Table(detail_header, colWidths=[4*inch, 2*inch])
    dt.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.silver),
        ('ALIGN', (1,0), (-1,-1), 'CENTER'),
        ('ALIGN', (0,1), (0,-1), 'LEFT'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
    ]))
    elements.append(dt)
    
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph("This is a computer-generated document.", ParagraphStyle('Footer', fontSize=8, alignment=1, textColor=colors.grey)))

    doc.build(elements)
    
    return send_file(filepath, as_attachment=True)

# ------------------ MASTER PRODUCTS MANAGEMENT ------------------
@app.route("/dairy/products", methods=["GET", "POST"])
def manage_products():
    if session.get("role") != "admin":
        return redirect("/")
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            try:
                cur.execute("INSERT INTO dairy_master_products (name, default_price) VALUES (%s, %s)",
                           (request.form["name"], request.form["default_price"]))
                conn.commit()
                flash("Product added successfully.")
            except Exception as e:
                flash(f"Error adding product: {str(e)}")
        elif action == "edit":
            try:
                prod_id = request.form["product_id"]
                cur.execute("UPDATE dairy_master_products SET name=%s, default_price=%s WHERE id=%s",
                           (request.form["name"], request.form["default_price"], prod_id))
                conn.commit()
                flash("Product updated successfully.")
            except Exception as e:
                flash(f"Error editing product: {str(e)}")
        elif action == "delete":
            prod_id = request.form["product_id"]
            cur.execute("DELETE FROM dairy_master_products WHERE id=%s", (prod_id,))
            conn.commit()
            flash("Product deleted.")
            
    cur.execute("SELECT * FROM dairy_master_products ORDER BY id ASC")
    products = cur.fetchall()
    conn.close()
    return render_template("dairy/manage_products.html", products=products)

# ------------------ CUSTOMER MANAGEMENT ------------------
@app.route("/dairy/customers", methods=["GET", "POST"])
def manage_customers():
    if session.get("role") != "admin":
        return redirect("/")
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    if request.method == "POST":
        data = request.get_json() if request.is_json else request.form
        action = data.get("action")
        
        def get_val(key, default=None):
            return data.get(key, default)
            
        def get_list(key):
            if request.is_json: return data.get(key, [])
            return request.form.getlist(key)

        if action == "add":
            try:
                # Reordering Logic
                new_order = int(get_val("delivery_order", 9999))
                cur.execute("UPDATE dairy_customers SET delivery_order = delivery_order + 1 WHERE delivery_order >= %s", (new_order,))
                
                # Insert Customer
                raw_password = get_val("password", "1234")
                hashed_pw = generate_password_hash(raw_password)
                cur.execute("""
                    INSERT INTO dairy_customers 
                    (name, phone, address, default_qty, price_per_liter, service_charge, delivery_staff_id, password, delivery_order, billing_type, last_bill_generated_on, net_payable)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    get_val("name"), get_val("phone"), get_val("address"), 
                    0, 0,
                    get_val("service_charge", 0),
                    get_val("delivery_staff_id", None) or None,
                    hashed_pw,
                    new_order,
                    get_val("billing_type", "Month End"),
                    None,
                    get_val("net_payable", 0)
                ))
                new_customer_id = cur.fetchone()[0]

                # Insert Products
                prod_names = get_list("product_name[]")
                prod_qtys = get_list("product_qty[]")
                prod_prices = get_list("product_price[]")
                
                for i in range(len(prod_names)):
                    if prod_names[i]:
                        cur.execute("""
                            INSERT INTO customer_products (customer_id, product_name, default_qty, price, delivery_order)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (new_customer_id, prod_names[i], prod_qtys[i] or 0, prod_prices[i] or 0, i))
                
                conn.commit()
            except Exception as e:
                print("Error adding customer:", e)
                conn.rollback()

        elif action == "edit":
            try:
                cust_id = get_val("customer_id")
                new_order = int(get_val("delivery_order", 9999))
                
                # Slot Reordering Logic
                cur.execute("SELECT delivery_order FROM dairy_customers WHERE id=%s", (cust_id,))
                old_order_res = cur.fetchone()
                if old_order_res:
                    old_order = old_order_res[0]
                    if new_order != old_order:
                        if new_order < old_order:
                            cur.execute("UPDATE dairy_customers SET delivery_order = delivery_order + 1 WHERE delivery_order >= %s AND delivery_order < %s", (new_order, old_order))
                        else:
                            cur.execute("UPDATE dairy_customers SET delivery_order = delivery_order - 1 WHERE delivery_order > old_order AND delivery_order <= new_order", (old_order, new_order))

                raw_password = get_val("password")
                if raw_password and not raw_password.startswith("scrypt:"): # Avoid hashing an existing hash
                    hashed_pw = generate_password_hash(raw_password)
                    cur.execute("""
                        UPDATE dairy_customers
                        SET name=%s, phone=%s, address=%s, price_per_liter=%s, service_charge=%s, delivery_staff_id=%s, password=%s, delivery_order=%s, billing_type=%s, last_bill_generated_on=%s, net_payable=%s
                        WHERE id=%s
                    """, (
                        get_val("name"), get_val("phone"), get_val("address"), 
                        get_val("price_per_liter", 0),
                        get_val("service_charge", 0),
                        get_val("delivery_staff_id") if get_val("delivery_staff_id") else None,
                        hashed_pw, new_order, 
                        get_val("billing_type", "current_month"), 
                        get_val("last_bill_generated_on") or None,
                        get_val("net_payable", 0),
                        cust_id
                    ))
                else:
                    cur.execute("""
                        UPDATE dairy_customers
                        SET name=%s, phone=%s, address=%s, price_per_liter=%s, service_charge=%s, delivery_staff_id=%s, delivery_order=%s, billing_type=%s, last_bill_generated_on=%s, net_payable=%s
                        WHERE id=%s
                    """, (
                        get_val("name"), get_val("phone"), get_val("address"), 
                        get_val("price_per_liter", 0),
                        get_val("service_charge", 0),
                        get_val("delivery_staff_id") if get_val("delivery_staff_id") else None,
                        new_order, 
                        get_val("billing_type", "current_month"), 
                        get_val("last_bill_generated_on") or None,
                        get_val("net_payable", 0),
                        cust_id
                    ))

                # Update Products (Delete & Re-insert)
                cur.execute("DELETE FROM customer_products WHERE customer_id=%s", (cust_id,))
                
                prod_names = get_list("product_name[]")
                prod_qtys = get_list("product_qty[]")
                prod_prices = get_list("product_price[]")
                
                for i in range(len(prod_names)):
                    if prod_names[i]:
                        cur.execute("""
                            INSERT INTO customer_products (customer_id, product_name, default_qty, price, delivery_order)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (cust_id, prod_names[i], prod_qtys[i] or 0, prod_prices[i] or 0, i))

                conn.commit()
            except Exception as e:
                print("Error editing customer:", e)
                conn.rollback()

        elif action == "delete":
            cid = request.form["customer_id"]
            # Close gap in slots
            cur.execute("SELECT delivery_order FROM dairy_customers WHERE id=%s", (cid,))
            res = cur.fetchone()
            if res:
                old_order = res[0]
                cur.execute("UPDATE dairy_customers SET delivery_order = delivery_order - 1 WHERE delivery_order > %s", (old_order,))

            cur.execute("DELETE FROM customer_products WHERE customer_id=%s", (cid,))
            cur.execute("DELETE FROM dairy_customers WHERE id=%s", (cid,))
            conn.commit()
    
    # Fetch Customers using the standard 16-column schema
    cur.execute("""
        SELECT id, name, phone, address, product_name, default_qty, price_per_liter, service_charge, 
               delivery_staff_id, password, delivery_order, billing_type, 
               last_bill_date, last_bill_generated_on, net_payable, email 
        FROM dairy_customers 
        ORDER BY delivery_order ASC, id DESC
    """)
    customers = cur.fetchall()
    
    # Fetch Products and Staff for each customer
    customer_data = []
    for c in customers:
        # Get staff name
        staff_name = "Unassigned"
        if c[8]: # delivery_staff_id
            cur.execute("SELECT name FROM delivery_staff WHERE id=%s", (c[8],))
            s = cur.fetchone()
            if s: staff_name = s[0]
            
        cur.execute("SELECT id, product_name, default_qty, price FROM customer_products WHERE customer_id=%s ORDER BY delivery_order", (c[0],))
        products = cur.fetchall()
        
        c_list = list(c)
        # Stringify dates to ensure JSON serialization
        if c_list[12]: c_list[12] = str(c_list[12])
        if c_list[13]: c_list[13] = str(c_list[13])
        
        c_list.append(staff_name) # Index 16
        c_list.append(products)   # Index 17
        customer_data.append(c_list)

    cur.execute("SELECT * FROM delivery_staff")
    staff_list = cur.fetchall()
    
    cur.execute("SELECT name, default_price FROM dairy_master_products ORDER BY id ASC")
    master_products = cur.fetchall()
    
    conn.close()
    
    if request.headers.get("Accept") == "application/json":
        return jsonify({"success": True, "customers": customer_data, "master_products": [list(x) for x in master_products]})
    
    # Serialize for JS
    master_products_json = json.dumps([{"name": mp[0], "defaultPrice": mp[1]} for mp in master_products])
    
    return render_template("dairy/manage_customers.html", customers=customer_data, staff_list=staff_list, master_products_json=master_products_json)

# ------------------ MONTHLY GRID SHEET ------------------
# [FIRST DEFINITION DELETED - REPLACED BY ATTENDANCE_SHEET BELOW]

# ------------------ REQUESTS / DISPUTES ------------------
@app.route("/dairy/request", methods=["POST"])
def submit_request():
    if session.get("role") != "customer":
        return redirect("/")
    
    customer_id = session.get("customer_id")
    # Handle optional fields for general requests
    new_date = request.form.get("new_date")
    if not new_date: new_date = None # PostgreSQL requires NULL for empty dates
    
    new_slot = request.form.get("new_slot", "AM")
    new_qty = request.form.get("new_qty")
    if not new_qty: new_qty = 0 # Handle empty quantity
    
    product_name = request.form.get("product_name")
    reason = request.form.get("reason", "No reason provided")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO attendance_requests (customer_id, new_date, new_time_slot, new_quantity, product_name, reason)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (customer_id, new_date, new_slot, new_qty, product_name, reason))
        conn.commit()
        flash("Success! Your request has been submitted.")
    except Exception as e:
        print(f"DEBUG: Error submitting request: {e}")
        conn.rollback()
        flash("An error occurred while submitting your request. Please try again.")
    finally:
        conn.close()
    
    return redirect(request.referrer or "/customer")

@app.route("/dairy/requests")
def view_requests():
    if session.get("role") != "admin":
        return redirect("/")
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Auto-cleanup: Remove non-pending requests older than 2 days
    cur.execute("DELETE FROM attendance_requests WHERE status != 'Pending' AND created_at < CURRENT_TIMESTAMP - INTERVAL '2 days'")
    conn.commit()

    cur.execute("""
        SELECT r.id, COALESCE(c.name, 'Admin/Staff'), r.new_date, r.new_time_slot, r.new_quantity, r.reason, r.status, r.customer_id, r.admin_response, r.product_name
        FROM attendance_requests r
        LEFT JOIN dairy_customers c ON r.customer_id = c.id
        ORDER BY r.id DESC
    """)
    all_requests = cur.fetchall()
    conn.close()
    
    # Group requests by status
    requests_dict = {
        'Pending': [r for r in all_requests if r[6] == 'Pending'],
        'Approved': [r for r in all_requests if r[6] == 'Approved'],
        'Rejected': [r for r in all_requests if r[6] == 'Rejected']
    }
    
    return render_template("dairy/manage_requests.html", requests=requests_dict)

@app.route("/dairy/request/action", methods=["POST"])
def action_request():
    if session.get("role") != "admin":
        return redirect("/")
        
    req_id = request.form["req_id"]
    action = request.form["action"]
    response = request.form.get("admin_response", "")
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if action == "approve":
            cur.execute("SELECT customer_id, new_date, new_time_slot, new_quantity, product_name FROM attendance_requests WHERE id=%s", (req_id,))
            req = cur.fetchone()
            if req:
                cid, date, slot, qty, prod_name = req
                # PostgreSQL safe check: only update logs if we have date/qty
                if date and slot and qty is not None:
                    # Find matching product_id for this customer
                    cur.execute("SELECT id FROM customer_products WHERE customer_id=%s AND product_name=%s", (cid, prod_name))
                    p_row = cur.fetchone()
                    pid = p_row[0] if p_row else None
                    
                    if pid:
                        cur.execute("SELECT id FROM dairy_logs WHERE customer_id=%s AND date=%s AND time_slot=%s AND product_id=%s", (cid, date, slot, pid))
                        existing = cur.fetchone()
                        if existing:
                            cur.execute("UPDATE dairy_logs SET quantity=%s WHERE id=%s", (qty, existing[0]))
                        else:
                            cur.execute("INSERT INTO dairy_logs (customer_id, date, time_slot, quantity, product_id) VALUES (%s, %s, %s, %s, %s)", (cid, date, slot, qty, pid))
            cur.execute("UPDATE attendance_requests SET status='Approved', admin_response=%s WHERE id=%s", (response, req_id))
        else:
            cur.execute("UPDATE attendance_requests SET status='Rejected', admin_response=%s WHERE id=%s", (response, req_id))
            
        conn.commit()
    except Exception as e:
        print(f"DEBUG: Error in action_request: {e}")
        conn.rollback()
    finally:
        conn.close()
    return redirect("/dairy/requests")


@app.route("/dairy/request/delete", methods=["POST"])
def delete_request():
    if session.get("role") != "admin":
        return redirect("/")
    
    req_id = request.form.get("req_id")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM attendance_requests WHERE id=%s", (req_id,))
    conn.commit()
    conn.close()
    flash("Request record removed successfully.")
    return redirect("/dairy/requests")

@app.route("/dairy/report/monthly", methods=["GET", "POST"])
def monthly_report():
    if session.get("role") not in ["admin", "delivery"]:
        return redirect("/")
        
    conn = get_db_connection()
    cur = conn.cursor()
    if request.method == "POST":
        month_str = request.form.get("month") # YYYY-MM
        export_type = request.form.get("export_type", "csv")
        
        if month_str:
            year, month = map(int, month_str.split("-"))
            num_days = calendar.monthrange(year, month)[1]
            days = [f"{year}-{month:02d}-{d:02d}" for d in range(1, num_days + 1)]
            day_nums = [str(d) for d in range(1, num_days + 1)]

            # 1. Fetch Customers & Products (Same logic as sheet)
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
            cur.execute("SELECT product_id, date, quantity FROM dairy_logs WHERE date::TEXT LIKE %s", (f"{month_str}-%",))
            logs_raw = cur.fetchall()
            logs_map = {} # {pid: {date: qty}}
            for pid, d, qty in logs_raw:
                pid_str = str(pid)
                if pid_str not in logs_map: logs_map[pid_str] = {}
                logs_map[pid_str][d] = qty

            # 3. Fetch Extra Purchases
            cur.execute("SELECT customer_id, date, product_name, quantity, amount FROM dairy_extra_purchases WHERE date::TEXT LIKE %s", (f"{month_str}-%",))
            extras_raw = cur.fetchall()
            extras_map = {} # {cid: [str]}
            for cid, edate, epname, eqty, eamt in extras_raw:
                if cid not in extras_map: extras_map[cid] = []
                # Format: "DD: Prod(Qty)"
                # Fix for subscriptable date error
                day_str = edate.strftime("%d") if hasattr(edate, "strftime") else str(edate)[8:]
                extras_map[cid].append(f"{day_str}: {epname}({eqty})")

            # 4. Fetch Payments
            cur.execute("SELECT customer_id, payment_date, amount, payment_mode FROM dairy_payments WHERE month = %s", (month_str,))
            payments_raw = cur.fetchall()
            payments_map = {} # {cid: [str]}
            for cid, pdate, pamt, pmode in payments_raw:
                if cid not in payments_map: payments_map[cid] = []
                # Fix for subscriptable date error
                day_str = pdate.strftime("%d") if hasattr(pdate, "strftime") else str(pdate)[8:]
                payments_map[cid].append(f"{day_str}: ₹{pamt}({pmode})")

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
                            # Fix for subscriptable date error
                            day_str = d.strftime("%d") if hasattr(d, "strftime") else str(d)[8:]
                            row[day_str] = q
                            total_qty += q
                            if q > 0: present_days += 1
                            else: absent_days += 1
                        else:
                            # Fix for subscriptable date error
                            day_str = d.strftime("%d") if hasattr(d, "strftime") else str(d)[8:]
                            row[day_str] = ""
                    
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

            if export_type == "json":
                report_path = os.path.join("reports", "monthly", f"attendance_{month_str}.json")
                os.makedirs(os.path.dirname(report_path), exist_ok=True)
                with open(report_path, 'w', encoding='utf-8') as f:
                    json.dump(matrix_data, f, indent=4)
                return send_file(report_path, as_attachment=True, mimetype='application/json')
            else:
                report_path = os.path.join("reports", "monthly", f"attendance_{month_str}.csv")
                os.makedirs(os.path.dirname(report_path), exist_ok=True)
                
                headers = ["Customer", "Staff", "Product", "Default Qty"] + day_nums + ["Monthly Total", "Present (Days)", "Absent (Days)", "Extra Purchases", "Payments"]
                
                with open(report_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for row in matrix_data:
                        # Map day keys from 'YYYY-MM-DD' subpart to 'D'
                        # Actually matrix_data already has day subparts as keys like "01", "02"
                        # But headers are "1", "2"... I should align them.
                        # Let's re-map row keys to match headers if needed.
                        csv_row = {}
                        for h in headers:
                            if h in row: csv_row[h] = row[h]
                            elif len(h) == 1 or (len(h) == 2 and h.isdigit()):
                                # It's a day header, e.g., "1"
                                # Matrix row has "01", "02"...
                                day_key = h.zfill(2)
                                csv_row[h] = row.get(day_key, "")
                            else:
                                csv_row[h] = ""
                        writer.writerow(csv_row)
                        
                return send_file(report_path, as_attachment=True)
                
    conn.close()
    return render_template("dairy/monthly_report.html")

# ------------------ MONTHLY ATTENDANCE GRID / ACCOUNTS ------------------
@app.route("/dairy/sheet")
def dairy_attendance_sheet():
    if session.get("role") != "admin":
        return redirect("/")
    
    # Get month from query param or default to current month
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    year, month = map(int, month_str.split("-"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get all customers ordered by delivery_order
    cur.execute("""
        SELECT id, name, phone, address, product_name, default_qty, price_per_liter, service_charge, 
               delivery_staff_id, password, delivery_order, billing_type, 
               last_bill_date, last_bill_generated_on, net_payable, email 
        FROM dairy_customers 
        ORDER BY delivery_order ASC, id DESC
    """)
    customers_raw = cur.fetchall()
    
    # Enrich with staff name and products
    customers = []
    cid_to_first_pid = {}
    for c in customers_raw:
        # Get staff name
        staff_name = "Unassigned"
        if c[8]: # delivery_staff_id
            cur.execute("SELECT name FROM delivery_staff WHERE id=%s", (c[8],))
            s = cur.fetchone()
            if s: staff_name = s[0]
            
        cur.execute("SELECT id, product_name, default_qty, price FROM customer_products WHERE customer_id=%s ORDER BY delivery_order", (c[0],))
        products = cur.fetchall()
        c_list = list(c)
        c_list.append(staff_name) # Index 16
        c_list.append(products)   # Index 17
        customers.append(c_list)
        if products:
            cid_to_first_pid[c[0]] = products[0][0]

    # Calculate days in month
    num_days = calendar.monthrange(year, month)[1]
    days = [datetime.date(year, month, day) for day in range(1, num_days + 1)]
    
    # Fetch all logs for this month including product_id
    cur.execute("""
        SELECT customer_id, date, time_slot, quantity, product_id 
        FROM dairy_logs 
        WHERE to_char(date, 'YYYY-MM') = %s
    """, (month_str,))
    logs = cur.fetchall()
    
    # Build a map: {product_id: {date: qty}}
    # Note: We assume one entry per product per day (AM slot usually). 
    # If multiple slots, we might need to sum them or handle AM/PM. For now, summing or taking last.
    logs_map = {}
    for log in logs:
        cid, date, slot, qty, pid = log
        
        # Fallback for legacy logs without product_id
        if not pid:
            pid = cid_to_first_pid.get(cid)
            
        if pid:
            pid_str = str(pid)
            if pid_str not in logs_map:
                logs_map[pid_str] = {}
            # Stringify date key for JSON compatibility
            date_str = date.isoformat() if hasattr(date, "isoformat") else str(date)
            # If entry exists (e.g. AM and PM), sum them
            # logs_map[pid_str][date_str] = logs_map[pid_str].get(date_str, 0) + qty # If we want sum
            logs_map[pid_str][date_str] = qty # For now, just take value (likely single slot)
    
    # Fetch extra notes for this month
    cur.execute("SELECT customer_id, notes FROM dairy_extra_notes WHERE month=%s", (month_str,))
    extra_notes_data = cur.fetchall()
    extra_notes = {row[0]: row[1] for row in extra_notes_data}

    # Fetch structured extra purchases for this month
    cur.execute("SELECT id, customer_id, date, product_name, quantity, rate, amount FROM dairy_extra_purchases WHERE to_char(date, 'YYYY-MM') = %s", (month_str,))
    extra_purchases_db = cur.fetchall()
    extra_purchases = {}
    for row in extra_purchases_db:
        cid = row[1]
        if cid not in extra_purchases:
            extra_purchases[cid] = []
        # Stringify date for JSON consistency
        date_str = row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2])
        extra_purchases[cid].append({
            'id': row[0],
            'date': date_str,
            'product': row[3],
            'qty': row[4],
            'rate': row[5],
            'amount': row[6]
        })

    # Get all master products for the "Extra Milk" dropdown
    cur.execute("SELECT name FROM dairy_master_products ORDER BY name ASC")
    all_product_names = [r[0] for r in cur.fetchall()]
    
    # Fetch payments for this month
    cur.execute("SELECT id, customer_id, payment_date, amount, payment_mode FROM dairy_payments WHERE month=%s", (month_str,))
    payments_data = cur.fetchall()
    payments = {}
    for row in payments_data:
        cid = row[1]
        if cid not in payments:
            payments[cid] = []
        # Stringify date for JSON consistency
        date_str = row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2])
        payments[cid].append({
            'id': row[0],
            'date': date_str,
            'amount': row[3],
            'mode': row[4]
        })
    
    # Fetch monthly P/A totals and Total Qty for all PRODUCTS
    monthly_totals = {} # {product_id: {'P': 0, 'A': 0, 'Total': 0}}
    cur.execute("""
        SELECT product_id, 
               COUNT(CASE WHEN quantity > 0 THEN 1 END) as present,
               COUNT(CASE WHEN quantity = 0 THEN 1 END) as absent,
               SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) as total_qty
        FROM dairy_logs 
        WHERE to_char(date, 'YYYY-MM') = %s AND product_id IS NOT NULL
        GROUP BY product_id
    """, (month_str,))
    for row in cur.fetchall():
        pid, p_cnt, a_cnt, t_qty = row
        monthly_totals[str(pid)] = {'P': p_cnt, 'A': a_cnt, 'Total': t_qty or 0}
        
    # Check for unpaid customers (net_payable > 0 and no payment record for previous month)
    # The requirement: highlight if date > 15th and they haven't paid their prior generated bill.
    # The prior generated bill is stored in net_payable.
    unpaid_customers = []
    today_date = datetime.date.today()
    
    if today_date.day > 15:
        # We need to see if they paid. Since net_payable tracks the most recent generated bill (usually the previous month's bill),
        # we check if they've made ANY payment in the *current* month_str view, OR we just check if net_payable > 0 and they haven't cleared it.
        # Let's say: if net_payable > 0 and they have NO payment record in `payments` dict for the current selected month.
        cur.execute("SELECT id, net_payable FROM dairy_customers WHERE net_payable > 0")
        for cid, net_payable in cur.fetchall():
            # If they don't have a payment recorded in the currently viewed month, flag them.
            # (Admins typically log the payment in the current month's sheet when they collect it).
            total_paid = sum(float(p.get('amount') or 0) for p in payments.get(cid, []))
            if cid not in payments or total_paid < net_payable:
                unpaid_customers.append(cid)
    
    conn.close()
    return render_template("dairy/attendance_sheet.html", 
                         customers=customers, 
                         days=days, 
                         logs_map=logs_map, 
                         extra_notes=extra_notes,
                         extra_purchases=extra_purchases,
                         all_product_names=all_product_names,
                         payments=payments,
                         monthly_totals=monthly_totals,
                         unpaid_customers=unpaid_customers,
                         month=month_str,
                         month_str=month_str,
                         role="admin")

@app.route("/delivery/history", methods=["GET"])
def delivery_history():
    if session.get("role") != "delivery":
        return redirect("/")
    
    staff_id = session.get("staff_id")
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    year, month = map(int, month_str.split("-"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get assigned customers
    cur.execute("""
        SELECT id, name, phone, address, product_name, default_qty, price_per_liter, service_charge, 
               delivery_staff_id, password, delivery_order, billing_type, 
               last_bill_date, last_bill_generated_on, net_payable, email 
        FROM dairy_customers 
        WHERE delivery_staff_id=%s
        ORDER BY delivery_order ASC, id ASC
    """, (staff_id,))
    customers_raw = cur.fetchall()
    
    # Get staff name for consistency
    cur.execute("SELECT name FROM delivery_staff WHERE id=%s", (staff_id,))
    s_row = cur.fetchone()
    staff_name = s_row[0] if s_row else "Unassigned"

    # Enrich
    customers = []
    cid_to_first_pid = {}
    for c in customers_raw:
        cur.execute("SELECT id, product_name, default_qty, price FROM customer_products WHERE customer_id=%s ORDER BY delivery_order", (c[0],))
        products = cur.fetchall()
        c_list = list(c)
        c_list.append(staff_name) # Index 16
        c_list.append(products)   # Index 17
        customers.append(c_list)
        if products:
            cid_to_first_pid[c[0]] = products[0][0]
    
    num_days = calendar.monthrange(year, month)[1]
    days = [datetime.date(year, month, day) for day in range(1, num_days + 1)]
    
    logs_map = {} # {product_id: {date: qty}}
    monthly_totals = {} # {product_id: {P:x, A:y, Total:z}}
    extra_notes = {}
    
    if customers:
        customer_ids = [c[0] for c in customers]
        cust_placeholders = ','.join('%s' for _ in customer_ids)
        
        # Fetch logs for assigned customers
        # Need to fetch by product_id primarily, but filtering by customer_id helps performance
        cur.execute(f"""
            SELECT customer_id, date, time_slot, quantity, product_id
            FROM dairy_logs 
            WHERE to_char(date, 'YYYY-MM') = %s AND customer_id IN ({cust_placeholders})
        """, [month_str] + customer_ids)
        logs = cur.fetchall()
        
        for log in logs:
            cid, date, slot, qty, pid = log
            if not pid:
                pid = cid_to_first_pid.get(cid)
                
            if pid:
                pid_str = str(pid)
                if pid_str not in logs_map:
                    logs_map[pid_str] = {}
                # Stringify date key for JSON compatibility
                date_str = date.isoformat() if hasattr(date, "isoformat") else str(date)
                logs_map[pid_str][date_str] = qty
        
        # Monthly Totals per Product
        cur.execute(f"""
            SELECT product_id, 
                   COUNT(CASE WHEN quantity > 0 THEN 1 END) as present,
                   COUNT(CASE WHEN quantity = 0 THEN 1 END) as absent,
                   SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) as total_qty
            FROM dairy_logs 
            WHERE to_char(date, 'YYYY-MM') = %s AND customer_id IN ({cust_placeholders}) AND product_id IS NOT NULL
            GROUP BY product_id
        """, [month_str] + customer_ids)
        for row in cur.fetchall():
            pid, p_cnt, a_cnt, t_qty = row
            monthly_totals[str(pid)] = {'P': p_cnt, 'A': a_cnt, 'Total': t_qty or 0}

        # Fetch extra notes
        cur.execute(f"SELECT customer_id, notes FROM dairy_extra_notes WHERE month=%s AND customer_id IN ({cust_placeholders})", [month_str] + customer_ids)
        for row in cur.fetchall():
            extra_notes[row[0]] = row[1]
    
    # Fetch extra purchases for these customers
    extra_purchases = {}
    if customers:
        cur.execute(f"SELECT id, customer_id, date, product_name, quantity, rate, amount FROM dairy_extra_purchases WHERE to_char(date, 'YYYY-MM') = %s AND customer_id IN ({cust_placeholders})", [month_str] + customer_ids)
        for row in cur.fetchall():
            cid = row[1]
            if cid not in extra_purchases: extra_purchases[cid] = []
            # Stringify date for JSON consistency
            date_str = row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2])
            extra_purchases[cid].append({'id': row[0], 'date': date_str, 'product': row[3], 'qty': row[4], 'rate': row[5], 'amount': row[6]})

    # Get all master products
    cur.execute("SELECT name FROM dairy_master_products ORDER BY name ASC")
    all_product_names = [r[0] for r in cur.fetchall()]

    conn.close()
    return render_template("dairy/attendance_sheet.html", 
                          customers=customers, 
                          days=days, 
                          logs_map=logs_map, 
                          extra_notes=extra_notes,
                          extra_purchases=extra_purchases,
                          all_product_names=all_product_names,
                          payments={},
                          monthly_totals=monthly_totals,
                          month=month_str,
                          month_str=month_str,
                          role="delivery")

# ------------------ AJAX ATTENDANCE TOGGLE ------------------
@app.route("/api/attendance/toggle", methods=["POST"])
def toggle_attendance():
    if session.get("role") not in ["admin", "delivery"]:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    
    conn = None
    try:
        data = request.get_json()
        raw_cid = data.get("customer_id")
        raw_pid = data.get("product_id")
        date = data.get("date")
        slot = data.get("slot", "AM")
        quantity = data.get("quantity")
        
        # Explicit type casting for PostgreSQL compatibility
        customer_id = int(raw_cid) if raw_cid else None
        product_id = int(raw_pid) if raw_pid else None
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if log exists
        if product_id:
            cur.execute("SELECT id FROM dairy_logs WHERE product_id=%s AND date=%s", (product_id, date))
        else:
            cur.execute("SELECT id FROM dairy_logs WHERE customer_id=%s AND date=%s AND time_slot=%s", (customer_id, date, slot))
            
        existing = cur.fetchone()
        
        if quantity is not None and quantity != '':
            qty_val = float(quantity)
            if qty_val > 0:
                if existing:
                    cur.execute("UPDATE dairy_logs SET quantity=%s WHERE id=%s", (qty_val, existing[0]))
                else:
                    cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, time_slot, quantity) VALUES (%s, %s, %s, %s, %s)",
                               (customer_id, product_id, date, slot, qty_val))
                status = "Present"
            elif qty_val == 0:
                if existing:
                    cur.execute("UPDATE dairy_logs SET quantity=0 WHERE id=%s", (existing[0],))
                else:
                    cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, time_slot, quantity) VALUES (%s, %s, %s, %s, 0)",
                               (customer_id, product_id, date, slot))
                status = "Absent"
            else:
                if existing:
                    cur.execute("DELETE FROM dairy_logs WHERE id=%s", (existing[0],))
                status = "Cleared"
        else:
            if existing:
                cur.execute("DELETE FROM dairy_logs WHERE id=%s", (existing[0],))
            status = "Cleared"
        
        conn.commit()
        return jsonify({"success": True, "status": status})
    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR] toggle_attendance: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/api/attendance/auto_fill", methods=["POST"])
def api_auto_fill():
    """Batch store daily attendance using default quantities for specified date."""
    if session.get("role") not in ["admin", "delivery"]:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    
    conn = None
    try:
        data = request.get_json() or {}
        date = data.get("date") or datetime.date.today().isoformat()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Fetch all default products for all customers
        cur.execute("SELECT customer_id, id, default_qty FROM customer_products")
        products = cur.fetchall()
        
        count = 0
        for cid, pid, dqty in products:
            if not dqty or dqty <= 0: continue
            
            # Check if already exists for this date (AM slot)
            cur.execute("SELECT id FROM dairy_logs WHERE product_id=%s AND date=%s", (pid, date))
            if not cur.fetchone():
                cur.execute("INSERT INTO dairy_logs (customer_id, product_id, date, time_slot, quantity) VALUES (%s, %s, %s, 'AM', %s)",
                           (cid, pid, date, dqty))
                count += 1
                
        conn.commit()
        return jsonify({"success": True, "inserted": count, "date": date})
    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR] api_auto_fill: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/api/attendance/extra", methods=["POST"])
def save_extra_note():
    if session.get("role") not in ["admin", "delivery"]:
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    
    data = request.get_json()
    customer_id = data.get("customer_id")
    month = data.get("month")
    notes = data.get("notes")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT id FROM dairy_extra_notes WHERE customer_id=%s AND month=%s", (customer_id, month))
    existing = cur.fetchone()
    
    if existing:
        cur.execute("UPDATE dairy_extra_notes SET notes=%s WHERE id=%s", (notes, existing[0]))
    else:
        cur.execute("INSERT INTO dairy_extra_notes (customer_id, month, notes) VALUES (%s, %s, %s)", 
                   (customer_id, month, notes))
    
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/stock/items", methods=["GET"])
def api_stock_items():
    if session.get("role") != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, stock FROM snacks_menu ORDER BY name ASC")
    items = cur.fetchall()
    conn.close()
    return jsonify({"items": items})

@app.route("/api/extra_purchase/save", methods=["POST"])
def save_extra_purchase():
    data = request.json
    customer_id = data.get('customer_id')
    date = data.get('date')
    product_name = data.get('product_name')
    quantity = data.get('quantity')
    rate = data.get('rate')
    
    if not all([customer_id, date, product_name, quantity, rate]):
        return jsonify({"success": False, "error": "Missing data"}), 400
        
    try:
        cid = int(customer_id)
        qty = float(quantity)
        rt = float(rate)
        amount = qty * rt
        
        print(f"DEBUG: Saving extra purchase for CID:{cid}, Date:{date}, Item:{product_name}, Qty:{qty}, Rate:{rt}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dairy_extra_purchases (customer_id, date, product_name, quantity, rate, amount)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (cid, date, product_name, qty, rt, amount))
        new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        
        return jsonify({
            "success": True, 
            "extra": {
                "id": new_id,
                "date": date,
                "product": product_name,
                "qty": qty,
                "rate": rt,
                "amount": amount
            }
        })
    except Exception as e:
        print(f"DEBUG ERROR in save_extra_purchase: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/extra_purchase/delete", methods=["POST"])
def delete_extra_purchase():
    data = request.json
    purchase_id = data.get('id')
    
    if not purchase_id:
        return jsonify({"success": False, "error": "Missing ID"}), 400
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM dairy_extra_purchases WHERE id=%s", (purchase_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/attendance/payment", methods=["POST"])
def save_payment():
    if session.get("role") != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403
    
    data = request.get_json()
    customer_id = data.get("customer_id")
    month = data.get("month")
    payment_date = data.get("payment_date")
    amount = data.get("amount")
    payment_mode = data.get("payment_mode")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Always insert a new record to support multiple payments for the same month
    cur.execute("""
        INSERT INTO dairy_payments (customer_id, month, payment_date, amount, payment_mode) 
        VALUES (%s, %s, %s, %s, %s)
    """, (customer_id, month, payment_date, amount, payment_mode))
    
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# ------------------ CUSTOMER EXPORT (PDF/CSV) ------------------
@app.route("/dairy/customer/export/<int:customer_id>", methods=["GET"])
def export_customer(customer_id):
    if session.get("role") not in ["admin", "delivery"]:
        return redirect("/")
    
    export_type = request.args.get("type", "csv")
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get customer info
    cur.execute("SELECT * FROM dairy_customers WHERE id=%s", (customer_id,))
    customer = cur.fetchone()
    
    if not customer:
        conn.close()
        return "Customer not found", 404
    
    # Get logs for this customer and month with product info
    cur.execute("""
        SELECT l.date, cp.product_name, l.quantity 
        FROM dairy_logs l
        LEFT JOIN customer_products cp ON l.product_id = cp.id
        WHERE l.customer_id=%s AND to_char(l.date, 'YYYY-MM') = %s
        ORDER BY l.date, cp.product_name
    """, (customer_id, month_str))
    logs = cur.fetchall()
    
    # Get extra notes
    cur.execute("SELECT notes FROM dairy_extra_notes WHERE customer_id=%s AND month=%s", (customer_id, month_str))
    extra_row = cur.fetchone()
    extra_notes_text = extra_row[0] if extra_row else ""
    
    conn.close()
    
    customer_name = customer[1]
    
    if export_type == "pdf":
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        
        # Sanitize name
        sanitized_name = customer_name.replace(" ", "_").replace("/", "-").replace("\\", "-")
        filename = f"{sanitized_name}_{month_str}_attend.pdf"
        filepath = os.path.join("reports", "customer", filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title = Paragraph(f"<b>Attendance Report: {customer_name}</b>", styles['Title'])
        elements.append(title)
        elements.append(Spacer(1, 0.2*inch))
        
        subtitle = Paragraph(f"Month: {month_str}", styles['Normal'])
        elements.append(subtitle)
        elements.append(Spacer(1, 0.3*inch))
        
        # Table data
        data = [["Date", "Product", "Quantity"]]
        for log in logs:
            data.append([log[0], log[1] or "N/A", str(log[2])])
        
        if len(data) == 1:
            data.append(["No records", "", ""])
        
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(table)
        
        if extra_notes_text:
            elements.append(Spacer(1, 0.3*inch))
            elements.append(Paragraph(f"<b>Extra Purchases:</b> {extra_notes_text}", styles['Normal']))
            
        doc.build(elements)
        
        return send_file(filepath, as_attachment=True, download_name=filename, mimetype='application/pdf')
    
    else:  # CSV
        # Sanitize name
        sanitized_name = customer_name.replace(" ", "_").replace("/", "-").replace("\\", "-")
        filename = f"{sanitized_name}_{month_str}_attend.csv"
        filepath = os.path.join("reports", "customer", filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([f"Customer: {customer_name}"])
            writer.writerow([f"Month: {month_str}"])
            writer.writerow([])
            writer.writerow(["Date", "Product", "Quantity"])
            writer.writerows(logs)
            
            if extra_notes_text:
                writer.writerow([])
                writer.writerow(["Extra Purchases:", extra_notes_text])
        
        return send_file(filepath, as_attachment=True, download_name=filename)

@app.route("/dairy/generate_bill/<int:customer_id>", methods=["POST"])
def generate_dairy_bill(customer_id):
    if session.get("role") != "admin":
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Fetch customer details
    cur.execute("SELECT name, phone, address, billing_type, service_charge, last_bill_date, last_bill_generated_on, net_payable, last_bill_amount FROM dairy_customers WHERE id=%s", (customer_id,))
    customer = cur.fetchone()
    if not customer:
        conn.close()
        return jsonify({"status": "error", "message": "Customer not found"}), 404
    
    cname, cphone, caddress, btype, service_charge, last_bill_date_str, last_bill_generated_on, existing_net_payable, last_bill_amount = customer
    
    arrears = float(existing_net_payable or 0)
    
    # 2. Determine Period First
    month_param = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    gen_date_param = request.args.get("generation_date")

    try:
        generation_date = datetime.date.fromisoformat(gen_date_param) if gen_date_param else datetime.date.today()
    except:
        generation_date = datetime.date.today()
        
    # Handle recalculation: Subtract the old bill from the same month
    if last_bill_generated_on and last_bill_amount:
        try:
            last_gen_month = datetime.datetime.strptime(last_bill_generated_on, "%Y-%m-%d").strftime("%Y-%m")
            if last_gen_month == month_param:
                # Deduct the previously added amount from the running debt
                # to prevent double-counting upon recalculation.
                arrears = max(0.0, arrears - float(last_bill_amount))
        except Exception as e:
            print(f"Error reverting previous bill: {e}")

    # SIMPLIFIED ARREARS CALCULATION: Look explicitly at the Grid module's payments
    try:
        y, m = map(int, month_param.split("-"))
        prev_d = datetime.date(y, m, 1) - datetime.timedelta(days=1)
        prev_month_str = prev_d.strftime("%Y-%m")
    except:
        prev_month_str = ""

    # Check if a payment was marked in the grid for the previous month (or current month)
    cur.execute("SELECT SUM(amount) FROM dairy_payments WHERE customer_id=%s AND month IN (%s, %s)", (customer_id, prev_month_str, month_param))
    res = cur.fetchone()
    grid_payments = float(res[0] or 0)

    arrears -= grid_payments
    if arrears < 0: arrears = 0.0
    
    try:
        year, month = map(int, month_param.split("-"))
    except:
        year, month = generation_date.year, generation_date.month

    try:
        year, month = map(int, month_param.split("-"))
    except:
        year, month = generation_date.year, generation_date.month

    # -----------------------------------------------------------
    # BILLING PERIOD: the fixed window shown on the PDF header
    # For Reservation: strictly 16th of previous month → 15th of bill month
    # For Month End: 1st → end of previous month
    # For Current Month: 1st → end of bill month
    # -----------------------------------------------------------
    if btype == 'reservation':
        # Month selected = START of cycle.
        # e.g. March selected → 16 Mar to 15 Apr
        billing_period_start = datetime.date(year, month, 16)
        next_month = month + 1 if month < 12 else 1
        next_year  = year if month < 12 else year + 1
        billing_period_end = datetime.date(next_year, next_month, 15)
    elif btype == 'month_end':
        bill_month_start = datetime.date(year, month, 1)
        prev_last = bill_month_start - datetime.timedelta(days=1)
        billing_period_start = prev_last.replace(day=1)
        billing_period_end   = prev_last
    else:  # current_month
        billing_period_start = datetime.date(year, month, 1)
        billing_period_end   = datetime.date(year, month, calendar.monthrange(year, month)[1])

    # -----------------------------------------------------------
    # SCAN WINDOW: the window used to find absences/extras
    #
    # RESERVATION: incremental window.
    #   scan_start = day after last generation (or 16th of prev month for first bill)
    #   scan_end   = generation_date (admin's chosen calculation date)
    #
    # MONTH END: NON-incremental, always scans the FULL previous month.
    #   scan_start = billing_period_start (1st of previous month)
    #   scan_end   = billing_period_end   (last day of previous month)
    #   (The full month is always used regardless of last generation date)
    #
    # CURRENT MONTH: incremental, like Reservation but for the current month period.
    #   scan_start = day after last generation (or 1st of current month for first bill)
    #   scan_end   = generation_date (admin's chosen calculation date)
    # -----------------------------------------------------------
    if btype == 'month_end':
        # Always scan the full previous month — non-incremental
        scan_start = billing_period_start
        scan_end   = billing_period_end

    elif btype == 'reservation':
        # Incremental: from the day after last generation up to the calculation date
        if last_bill_generated_on:
            try:
                last_gen_d = datetime.date.fromisoformat(last_bill_generated_on)
                scan_start = last_gen_d + datetime.timedelta(days=1)
            except:
                prev_month = month - 1 if month > 1 else 12
                prev_year  = year if month > 1 else year - 1
                scan_start = datetime.date(prev_year, prev_month, 16)
        else:
            # First-ever bill: start from 16th of previous month
            prev_month = month - 1 if month > 1 else 12
            prev_year  = year if month > 1 else year - 1
            scan_start = datetime.date(prev_year, prev_month, 16)
        scan_end = generation_date  # strictly up to the chosen calculation date

    else:  # current_month
        # Incremental like Reservation but bounded inside the current month's billing period
        if last_bill_generated_on:
            try:
                last_gen_d = datetime.date.fromisoformat(last_bill_generated_on)
                scan_start = last_gen_d + datetime.timedelta(days=1)
            except:
                scan_start = billing_period_start
        else:
            # First-ever bill: start from the 1st of the current month
            scan_start = billing_period_start
        scan_end = generation_date  # up to the admin's chosen calculation date


    # For display purposes use the billing period
    period_start = billing_period_start
    period_end   = billing_period_end

    # 3. Fetch Data for the period
    cur.execute("SELECT id, product_name, default_qty, price FROM customer_products WHERE customer_id=%s", (customer_id,))
    products = cur.fetchall()
    
    attendance_data = [] # Detailed logs
    milk_summary = {} # {product_name: {'qty': 0, 'rate': 0, 'amount': 0}}
    total_product_bill = 0
    extra_purchases = [] # Logs where qty > dqty
    
    # Pre-calculate product info
    prod_map = {p[0]: {"name": p[1], "dqty": p[2], "price": p[3]} for p in products}

    # ---------------------------------------------------------------
    # Calculate the base bill from the FULL billing period
    # (always 16th to 15th for Reservation regardless of scan window)
    # This gives "expected" total for the month.
    # ---------------------------------------------------------------
    # ---------------------------------------------------------------
    # 1. Calculate the BASE MONTHLY BILL using DEFAULT quantities
    # (Matches the full billing period: e.g. 1st to 31st)
    # ---------------------------------------------------------------
    curr = billing_period_start
    while curr <= billing_period_end:
        for pid in prod_map:
            pinfo = prod_map[pid]
            dqty = float(pinfo["dqty"] or 0)
            price = float(pinfo["price"] or 0)

            if dqty > 0:
                amount = dqty * price
                total_product_bill += amount

                pname = pinfo["name"]
                if pname not in milk_summary:
                    milk_summary[pname] = {'qty': 0, 'rate': price, 'amount': 0}
                milk_summary[pname]['qty'] += dqty
                milk_summary[pname]['amount'] += amount

        curr += datetime.timedelta(days=1)

    # SECONLDY, Fetch all actual logs for the FULL period for the "Attendance Log" table in PDF
    # (Just for visual proof to the customer)
    curr = billing_period_start
    while curr <= billing_period_end:
        date_iso = curr.isoformat()
        for pid in prod_map:
            pinfo = prod_map[pid]
            cur.execute("SELECT quantity FROM dairy_logs WHERE product_id=%s AND date=%s", (pid, date_iso))
            log = cur.fetchone()
            
            # If log exists, use it; else use default for the visual log
            log_qty = float(log[0]) if (log and log[0] is not None and log[0] != '') else float(pinfo["dqty"] or 0)
            if log_qty > 0:
                l_amount = log_qty * float(pinfo["price"] or 0)
                attendance_data.append([curr.strftime("%d-%m-%Y"), pinfo["name"], f"{log_qty:.2f}", f"{float(pinfo['price']):.2f}", f"{l_amount:.2f}"])
        curr += datetime.timedelta(days=1)

    # Fetch Extra Monthly Notes (Manual additions)
    cur.execute("SELECT notes FROM dairy_extra_notes WHERE customer_id=%s AND month=%s", (customer_id, month_param))
    notes_record = cur.fetchone()
    other_notes = notes_record[0] if notes_record else ""

    # Service Charge
    cur.execute("SELECT SUM(default_qty) FROM customer_products WHERE customer_id = %s", (customer_id,))
    fixed_qty_sum = float(cur.fetchone()[0] or 0)
    service_bill = float(service_charge or 0) * fixed_qty_sum
    
    # Scan absences and extras ONLY within the moving scan window
    # (scan_start = last_generation+1, scan_end = generation_date)
    # This means each bill only deducts deviations since last generation.
    # ---------------------------------------------------------------
    deduction_details = []
    total_deduction = 0.0
    
    addition_details = [] # From logs (actual > default)
    total_additions = 0.0

    # Scan deviations (shortages / over-logs) ONLY within the moving scan window.
    # scan_start = last_generation_date + 1 (or start of cycle if first bill)
    # scan_end   = current generation_date (chosen by admin)
    curr_d = scan_start
    while curr_d <= scan_end:
        date_iso = curr_d.isoformat()
        for pid in prod_map:
            pinfo = prod_map[pid]
            dqty  = float(pinfo["dqty"] or 0)
            price = float(pinfo["price"] or 0)

            cur.execute("SELECT quantity FROM dairy_logs WHERE product_id=%s AND date=%s", (pid, date_iso))
            log = cur.fetchone()

            if log and log[0] is not None and log[0] != '':
                actual_qty = float(log[0])
                if actual_qty < dqty:
                    absent_qty    = dqty - actual_qty
                    deduct_amount = absent_qty * price
                    total_deduction += deduct_amount
                    deduction_details.append([
                        curr_d.strftime("%d-%m-%Y"),
                        pinfo["name"],
                        f"{absent_qty:.2f}",
                        f"{price:.2f}",
                        f"₹{deduct_amount:.2f}",
                        "Shortage"
                    ])
                elif actual_qty > dqty:
                    extra_qty = actual_qty - dqty
                    extra_amount = extra_qty * price
                    total_additions += extra_amount
                    addition_details.append([
                        curr_d.strftime("%d-%m-%Y"),
                        pinfo["name"],
                        f"{extra_qty:.2f}",
                        f"{price:.2f}",
                        f"₹{extra_amount:.2f}",
                        "Over-Log"
                    ])
        curr_d += datetime.timedelta(days=1)

    # Scan the dairy_extra_purchases table for the incremental window only
    # (from scan_start to scan_end, i.e., previous calc date+1 → current calculation date)
    cur.execute("SELECT id, date, product_name, quantity, rate, amount FROM dairy_extra_purchases WHERE customer_id=%s AND date BETWEEN %s AND %s", 
               (customer_id, scan_start.isoformat(), scan_end.isoformat()))
    extras_db = cur.fetchall()
    for eid, edate, epname, eqty, erate, eamt in extras_db:
        total_additions += float(eamt)
        # Parse date for consistent display
        try:
            edate_obj = datetime.date.fromisoformat(edate)
            display_date = edate_obj.strftime("%d-%m-%Y")
        except:
            display_date = edate
            
        addition_details.append([
            display_date,
            epname,
            f"{float(eqty):.2f}",
            f"{float(erate):.2f}",
            f"₹{float(eamt):.2f}",
            "Extra Purchase"
        ])

    current_charges = float(total_product_bill) + float(service_bill) + float(total_additions) - float(total_deduction)
    final_amount = current_charges + arrears

    # Previous cycle payment (for info only)
    prev_month_date = billing_period_start - datetime.timedelta(days=1)
    prev_month_str = prev_month_date.strftime("%Y-%m")
    cur.execute("SELECT payment_date, amount, payment_mode FROM dairy_payments WHERE customer_id=%s AND month=%s", 
               (customer_id, prev_month_str))
    prev_p = cur.fetchone()

    # 4. Generate PDF
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    
    # Sanitize name
    sanitized_name = cname.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"{sanitized_name}_{month_param}_bill.pdf"
    filepath = os.path.join("static", "bills", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    doc = SimpleDocTemplate(filepath, pagesize=A4, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('TitleStyle', parent=styles['Heading1'], fontSize=18, alignment=1, spaceAfter=20)
    header_style = ParagraphStyle('HeaderStyle', parent=styles['Normal'], fontSize=10, leading=12)
    section_style = ParagraphStyle('SectionStyle', parent=styles['Normal'], fontSize=12, leading=14, spaceBefore=15, spaceAfter=10, fontName='Helvetica-Bold')

    # Header
    elements.append(Paragraph("<b>HARI SAKTHI DAIRY AND SNACKS</b>", title_style))
    elements.append(Paragraph("<b>DAIRY BILL / INVOICE</b>", ParagraphStyle('Sub', alignment=1, fontSize=14, spaceAfter=10)))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=colors.black))
    elements.append(Spacer(1, 0.2*inch))

    # Customer & Bill Info
    info_data = [
        [Paragraph(f"<b>Customer Name:</b> {cname}", header_style), Paragraph(f"<b>Bill No:</b> HB-{customer_id}-{datetime.date.today().strftime('%y%m%d')}", header_style)],
        [Paragraph(f"<b>Phone:</b> {cphone or 'N/A'}", header_style), Paragraph(f"<b>Period:</b> {period_start.strftime('%d-%m-%Y')} to {period_end.strftime('%d-%m-%Y')}", header_style)],
        [Paragraph(f"<b>Address:</b> {caddress or 'N/A'}", header_style), Paragraph(f"<b>Billing Type:</b> {btype.replace('_', ' ').title()}", header_style)],
        ["", Paragraph(f"<b>Date:</b> {datetime.date.today().strftime('%d-%m-%Y')}", header_style)]
    ]
    info_table = Table(info_data, colWidths=[3.2*inch, 3.2*inch])
    info_table.setStyle(TableStyle([
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 0.2*inch))

    # Summary Table (Milk details amount)
    elements.append(Paragraph("<b>Milk Consumption Summary</b>", section_style))
    summary_table_data = [["Product Name", "Total Qty", "Rate", "Total Amount"]]
    for pname, vals in milk_summary.items():
        summary_table_data.append([pname, f"{float(vals['qty']):.2f}", f"₹{float(vals['rate']):.2f}", f"₹{float(vals['amount']):.2f}"])
    
    if not milk_summary:
        summary_table_data.append(["No Consumption", "0.00", "0.00", "₹0.00"])

    st = Table(summary_table_data, colWidths=[2.5*inch, 1.2*inch, 1.2*inch, 1.5*inch])
    st.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ALIGN', (1,0), (-1,-1), 'CENTER'),
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('TOPPADDING', (0,0), (-1,0), 8),
    ]))
    elements.append(st)

    # Attendance Details (if requested)
    elements.append(Paragraph("<b>Daily Attendance Log</b>", section_style))
    attendance_header = [["Date", "Product", "Qty", "Rate", "Amount"]]
    # Limit rows to avoid huge PDFs, or show as secondary
    display_attendance = attendance_data[:62] # Show up to 2 months essentially
    table_data = attendance_header + display_attendance
    at = Table(table_data, colWidths=[1.2*inch, 2.0*inch, 0.8*inch, 0.8*inch, 1.2*inch])
    at.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 0.2, colors.silver),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('ALIGN', (1,0), (-1,-1), 'LEFT'),
        ('ALIGN', (0,0), (0,-1), 'CENTER'),
        ('ALIGN', (2,0), (-1,-1), 'CENTER'),
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
    ]))
    elements.append(at)

    # Extra Purchases Section (Additions)
    if addition_details:
        elements.append(Paragraph("<b>Extra Purchases & Additions</b>", section_style))
        ex_header = [["Date", "Product", "Qty", "Rate", "Amount", "Reason"]]
        ext_table = Table(ex_header + addition_details, colWidths=[1.0*inch, 1.8*inch, 0.7*inch, 0.7*inch, 1.1*inch, 1.2*inch])
        ext_table.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTSIZE', (0,0), (-1,-1), 8),
            ('BACKGROUND', (0,0), (-1,0), colors.lightgreen),
            ('ALIGN', (0,0), (0,-1), 'CENTER'),
            ('ALIGN', (2,0), (-1,-1), 'CENTER'),
            ('TEXTCOLOR', (4,1), (4,-1), colors.black),
        ]))
        elements.append(ext_table)

    # Deductions Section
    if deduction_details:
        elements.append(Paragraph("<b>Deductions for Absent Days</b>", section_style))
        deduct_header = [["Date", "Product", "Absent Qty", "Rate", "Deducted Amount", "Period"]]
        deduct_table = Table(deduct_header + deduction_details, colWidths=[1.0*inch, 1.6*inch, 0.7*inch, 0.7*inch, 1.2*inch, 1.3*inch])
        deduct_table.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTSIZE', (0,0), (-1,-1), 8),
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#ffcccb')),
            ('ALIGN', (0,0), (0,-1), 'CENTER'),
            ('ALIGN', (2,0), (-1,-1), 'CENTER'),
            ('TEXTCOLOR', (4,1), (4,-1), colors.red),
        ]))
        elements.append(deduct_table)

    # Manual Notes
    if other_notes:
        elements.append(Paragraph("<b>Additional Notes:</b>", section_style))
        elements.append(Paragraph(other_notes, styles['Normal']))

    # Previous cycle info
    if prev_p:
        prev_amount = float(prev_p[1]) if prev_p[1] and prev_p[1] != '' else 0.0
        elements.append(Spacer(1, 0.2*inch))
        elements.append(Paragraph(f"<i>Note: Previous monthly bill ({prev_month_str}) of Rs. {prev_amount:.2f} was paid on {prev_p[0]} via {prev_p[2]}.</i>", 
                                 ParagraphStyle('Note', fontSize=9, textColor=colors.grey)))

    # Final Summary
    elements.append(Spacer(1, 0.3*inch))
    summary_rows = [
        ["Base Milk Bill:", f"Rs. {float(total_product_bill):.2f}"]
    ]
    if service_bill > 0:
        summary_rows.append(["Service Charge:", f"Rs. {float(service_bill):.2f}"])
    if total_additions > 0:
        summary_rows.append(["Extras / Additions:", f"+ Rs. {float(total_additions):.2f}"])
    if total_deduction > 0:
        summary_rows.append(["Deductions (Absent Days):", f"- Rs. {float(total_deduction):.2f}"])
    summary_rows.append(["Subtotal (Current Cycle):", f"Rs. {current_charges:.2f}"])
    if arrears > 0:
        summary_rows.append([Paragraph("Arrears (Previous Balance):", ParagraphStyle('Add', fontSize=10, textColor=colors.red)), f"Rs. {arrears:.2f}"])

    bold_label_style = ParagraphStyle('BoldLabel', parent=styles['Normal'], fontSize=11, alignment=2, fontName='Helvetica-Bold')
    bold_value_style = ParagraphStyle('BoldValue', parent=styles['Normal'], fontSize=11, fontName='Helvetica-Bold')
    summary_rows.append([Paragraph("TOTAL AMOUNT DUE:", bold_label_style), Paragraph(f"Rs. {float(final_amount):.2f}", bold_value_style)])
    
    sum_t = Table(summary_rows, colWidths=[4.5*inch, 1.9*inch])
    sum_t.setStyle(TableStyle([
        ('ALIGN', (0,0), (0,-1), 'RIGHT'),
        ('ALIGN', (1,0), (1,-1), 'LEFT'),
        ('FONTSIZE', (0,0), (-1,-1), 11),
        ('LINEABOVE', (0,-1), (-1,-1), 1, colors.black),
        ('TOPPADDING', (0,-1), (-1,-1), 5),
    ]))
    elements.append(sum_t)

    # Footer
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph("Thank you for choosing HARI SAKTHI DAIRY & SNACKS!", ParagraphStyle('Footer', alignment=1, fontSize=10, textColor=colors.darkgrey)))

    doc.build(elements)
    
    # Update last_bill_date — remember the generation_date so next scan starts from next day
    # Also save the net_payable amount.
    cur.execute("UPDATE dairy_customers SET last_bill_date=%s, last_bill_generated_on=%s, net_payable=%s, last_bill_amount=%s WHERE id=%s",
        (billing_period_end.isoformat(), generation_date.isoformat(), final_amount, current_charges, customer_id))
    conn.commit()
    conn.close()
    
    return jsonify({
        "status": "success",
        "pdf_url": "/" + filepath.replace("\\", "/"),
        "filename": filename,
        "bill_amount": round(final_amount, 2),
        "period": f"{billing_period_start.strftime('%d/%m/%y')} - {billing_period_end.strftime('%d/%m/%y')}",
        "scan_window": f"{scan_start.strftime('%d/%m/%y')} - {scan_end.strftime('%d/%m/%y')}"
    })

# ------------------ LOGOUT ------------------
@app.route("/dairy/reset_billing/<int:customer_id>", methods=["POST"])
def reset_dairy_billing(customer_id):
    if session.get("role") != "admin":
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT net_payable, last_bill_amount FROM dairy_customers WHERE id=%s", (customer_id,))
    row = cur.fetchone()
    if row and row[1]:
        # Revert the net payable
        new_net = max(0, float(row[0] or 0) - float(row[1]))
        cur.execute("UPDATE dairy_customers SET last_bill_generated_on=NULL, last_bill_date=NULL, last_bill_amount=NULL, net_payable=%s WHERE id=%s", (new_net, customer_id))
    else:
        # Fallback if no last_bill_amount was stored
        cur.execute("UPDATE dairy_customers SET last_bill_generated_on=NULL, last_bill_date=NULL WHERE id=%s", (customer_id,))
        
    conn.commit()
    conn.close()
    
    return jsonify({"status": "success", "message": "Billing memory reset."})

@app.route("/dairy/payroll", methods=["GET", "POST"])
def staff_payroll():
    if session.get("role") != "admin":
        return redirect("/")
    
    month_str = request.args.get("month", datetime.date.today().strftime("%Y-%m"))
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        data = request.get_json()
        staff_id = data.get("staff_id")
        month = data.get("month")
        base = float(data.get("base_salary", 0))
        comm = float(data.get("commission", 0))
        deduct = float(data.get("deductions", 0))
        bonus = float(data.get("bonus", 0))
        total = base + comm + bonus - deduct
        mode = data.get("payment_mode", "Cash")
        notes = data.get("notes", "")
        p_date = datetime.date.today().isoformat()

        # Check if entry exists for this staff/month
        cur.execute("SELECT id FROM staff_payroll WHERE staff_id=%s AND month=%s", (staff_id, month))
        existing = cur.fetchone()
        
        if existing:
            cur.execute("""
                UPDATE staff_payroll SET base_salary=%s, commission=%s, deductions=%s, bonus=%s, total_paid=%s, payment_date=%s, payment_mode=%s, notes=%s
                WHERE id=%s
            """, (base, comm, deduct, bonus, total, p_date, mode, notes, existing[0]))
        else:
            cur.execute("""
                INSERT INTO staff_payroll (staff_id, month, base_salary, commission, deductions, bonus, total_paid, payment_date, payment_mode, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (staff_id, month, base, comm, deduct, bonus, total, p_date, mode, notes))
            
        conn.commit()
        conn.close()
        return jsonify({"success": True})

    # GET: Fetch staff list and their existing payroll for the month
    cur.execute("SELECT id, name, username FROM delivery_staff")
    staff_rows = cur.fetchall()
    
    # Calculate previous month for distribution total
    try:
        curr_y, curr_m = map(int, month_str.split('-'))
        prev_date = datetime.date(curr_y, curr_m, 1) - datetime.timedelta(days=1)
        prev_month_str = prev_date.strftime("%Y-%m")
    except:
        prev_month_str = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")

    staff_list = []
    for s_id, s_name, s_user in staff_rows:
        # Calculate total distribution in previous month across all customers assigned to this staff
        # Grouped by dairy_logs.date to ensure we sum correctly across the month
        cur.execute("""
            SELECT SUM(l.quantity) 
            FROM dairy_logs l
            JOIN dairy_customers c ON l.customer_id = c.id
            WHERE c.delivery_staff_id = %s AND to_char(l.date, 'YYYY-MM') = %s
        """, (s_id, prev_month_str))
        total_qty = cur.fetchone()[0] or 0.0
        
        staff_list.append({
            "id": s_id,
            "name": s_name,
            "username": s_user,
            "prev_dist_qty": round(total_qty, 1),
            "prev_month_label": prev_date.strftime("%B %Y") if 'prev_date' in locals() else "Previous Month"
        })
    
    cur.execute("SELECT * FROM staff_payroll WHERE month=%s", (month_str,))
    payroll_rows = cur.fetchall()
    columns = [column[0] for column in cur.description]
    payroll_entries = [dict(zip(columns, row)) for row in payroll_rows]
    
    conn.close()
    return render_template("dairy/payroll.html", staff_list=staff_list, payroll_entries=payroll_entries, month_str=month_str)

@app.route("/api/stock/history")
def api_stock_history():
    if session.get("role") != "admin": return jsonify({"success": False}), 403
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT si.id, m.name, si.qty, si.purchase_price, si.supplier, si.notes, si.date, m.id as item_id
        FROM snacks_stock_in si
        JOIN snacks_menu m ON si.item_id = m.id
        ORDER BY si.date DESC LIMIT 100
    """)
    logs = [dict(zip(['id', 'name', 'qty', 'cost', 'supplier', 'notes', 'date', 'item_id'], row)) for row in cur.fetchall()]
    conn.close()
    return jsonify({"success": True, "logs": logs})

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ------------------ DAIRY API ------------------
@app.route("/api/dairy/payment/save", methods=["POST"])
def api_dairy_payment_save():
    if session.get("role") != "admin": return jsonify({"success": False, "error": "Unauthorized"}), 403
    data = request.get_json()
    cid = data.get("customer_id")
    month = data.get("month")
    p_date = data.get("payment_date")
    mode = data.get("mode")
    amount = data.get("amount", 0) # Defaults to 0
    
    if not all([cid, month, p_date, mode]):
        return jsonify({"success": False, "error": "Missing fields"}), 400
        
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dairy_payments (customer_id, month, payment_date, amount, payment_mode)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (cid, month, p_date, amount, mode))
        new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"success": True, "payment": {"id": new_id, "date": p_date, "mode": mode, "amount": amount}})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@app.route("/api/dairy/payment/delete", methods=["POST"])
def api_dairy_payment_delete():
    if session.get("role") != "admin": return jsonify({"success": False, "error": "Unauthorized"}), 403
    data = request.get_json() or {}
    pid = data.get("id")
    if pid is None: return jsonify({"success": False, "error": "Missing ID"}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM dairy_payments WHERE id = %s", (pid,))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@app.route("/api/extra_purchase/save", methods=["POST"])
def api_extra_purchase_save():
    if session.get("role") != "admin": return jsonify({"success": False, "error": "Unauthorized"}), 403
    data = request.get_json()
    cid = data.get("customer_id")
    pname = data.get("product_name")
    date = data.get("date")
    qty = data.get("quantity")
    rate = data.get("rate")
    
    if not all([cid, pname, date, qty, rate]):
        return jsonify({"success": False, "error": "Missing fields"}), 400
        
    amount = float(qty) * float(rate)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dairy_extra_purchases (customer_id, product_name, date, quantity, rate, amount)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (cid, pname, date, qty, rate, amount))
        new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({
            "success": True, 
            "extra": {"id": new_id, "product": pname, "date": date, "qty": qty, "rate": rate, "amount": amount}
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@app.route("/api/extra_purchase/delete", methods=["POST"])
def api_extra_purchase_delete():
    if session.get("role") != "admin": return jsonify({"success": False, "error": "Unauthorized"}), 403
    data = request.get_json() or {}
    eid = data.get("id")
    if eid is None: return jsonify({"success": False, "error": "Missing ID"}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM dairy_extra_purchases WHERE id = %s", (eid,))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

# ------------------ MAIN SERVER ------------------
# Run init_db at module level so Gunicorn (Render/Vercel) triggers it on startup
# init_db() is idempotent — uses CREATE TABLE IF NOT EXISTS
try:
    init_db()
except Exception as _init_err:
    print(f"[WARNING] init_db() failed at module load: {_init_err}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False, threaded=True)
