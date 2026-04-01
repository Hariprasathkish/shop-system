import sqlite3
import psycopg2
from db_config import DB_CONFIG

SQLITE_DB = 'shop_system.db'

# Order that respects foreign key dependencies
TABLES_ORDER = [
    'admin_users',
    'delivery_staff',
    'dairy_customers',
    'customer_products',
    'dairy_logs',
    'dairy_extra_notes',
    'dairy_payments',
    'dairy_extra_purchases',
    'dairy_master_products',
    'snacks_menu',
    'snacks_stock_in',
    'snacks_bills',
    'snacks_bill_items',
    'snacks_sales',
    'staff_payroll',
    'attendance_requests'
]

from app import init_db

def migrate():
    try:
        print("Initializing database schema via init_db()...")
        init_db()
        
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cur = sqlite_conn.cursor()

        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(**DB_CONFIG)
        print(f"  Connected to: {pg_conn.get_dsn_parameters()['dbname']}")
        pg_cur = pg_conn.cursor()

        # Disable triggers for foreign key safety
        pg_cur.execute("SET session_replication_role = 'replica';")

        # 1. Clear all tables first (in reverse order)
        for table in reversed(TABLES_ORDER):
            print(f"Cleaning table: {table}")
            pg_cur.execute(f"DELETE FROM \"{table}\";")

        # 2. Migrate all tables
        for table in TABLES_ORDER:
            print(f"Migrating table: {table}")
            
            # Get PostgreSQL column names for this table using description (more reliable)
            pg_cur.execute(f"SELECT * FROM \"{table}\" LIMIT 0")
            print(f"  PG Desc for {table}: {[d[0] for d in pg_cur.description]}")
            pg_cols = {desc[0] for desc in pg_cur.description}
            
            if not pg_cols:
                print(f"  Skipping {table}: Table not found in PostgreSQL")
                continue

            # Fetch SQLite rows as dictionaries
            sqlite_cur.execute(f"SELECT * FROM \"{table}\"")
            sqlite_rows = sqlite_cur.fetchall()
            if not sqlite_rows:
                print(f"  No data in {table}")
                continue

            # Find common columns
            sqlite_cols = set(sqlite_rows[0].keys())
            print(f"  PG Cols: {pg_cols}")
            print(f"  SQLite Cols: {sqlite_cols}")
            common_cols = sorted(list(pg_cols.intersection(sqlite_cols)))
            
            if not common_cols:
                print(f"  Skipping {table}: No common columns found")
                continue

            print(f"  Common columns for {table}: {common_cols}")

            # Prepare INSERT query
            placeholders = ", ".join(["%s"] * len(common_cols))
            col_list = ", ".join([f'"{c}"' for c in common_cols])
            insert_query = f"INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders})"
            
            # Prepare data
            data_to_insert = []
            pg_col_types = {}
            pg_cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
            for r in pg_cur.fetchall():
                pg_col_types[r[0]] = r[1]

            for s_row in sqlite_rows:
                vals = []
                for c in common_cols:
                    val = s_row[c]
                    target_type = pg_col_types.get(c, "")
                    
                    if val == "" or val is None:
                        val = None
                    elif target_type == 'integer':
                        try:
                            val = int(float(val))
                        except (ValueError, TypeError):
                            val = None
                    elif target_type in ('real', 'double precision', 'numeric'):
                        try:
                            val = float(val)
                        except (ValueError, TypeError):
                            val = None
                    elif target_type == 'date' and val:
                        # Ensure date is in YYYY-MM-DD format, handled by PG usually but cleaning empty strings
                        if not val or str(val).strip() == "":
                            val = None
                    
                    vals.append(val)
                data_to_insert.append(tuple(vals))

            # Batch insert
            pg_cur.executemany(insert_query, data_to_insert)
            print(f"  Inserted {len(sqlite_rows)} rows into {table}")

            # 3. Reset sequence for 'id' column if it exists
            if 'id' in pg_cols:
                pg_cur.execute(f"SELECT MAX(id) FROM \"{table}\"")
                max_id = pg_cur.fetchone()[0]
                if max_id:
                    pg_cur.execute(f"SELECT pg_get_serial_sequence('\"{table}\"', 'id')")
                    res = pg_cur.fetchone()
                    if res and res[0]:
                        seq_name = res[0]
                        pg_cur.execute(f"SELECT setval('{seq_name}', {max_id})")
                        print(f"  Reset sequence {seq_name} to {max_id}")

        # Re-enable triggers
        pg_cur.execute("SET session_replication_role = 'origin';")
        
        pg_conn.commit()
        print("\nMigration completed successfully!")

    except Exception as e:
        print(f"\nFATAL: Migration failed: {e}")
        if 'pg_conn' in locals(): pg_conn.rollback()
    finally:
        if 'sqlite_conn' in locals(): sqlite_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()

if __name__ == "__main__":
    migrate()
