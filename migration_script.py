import sqlite3
import psycopg2
from psycopg2 import sql
import sys
from db_config import DB_CONFIG

SQLITE_DB = "shop_system.db"

def migrate():
    try:
        # Connect to SQLite
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        sqlite_cur = sqlite_conn.cursor()

        # Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(**DB_CONFIG)
        pg_cur = pg_conn.cursor()

        # Get all tables from SQLite
        sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in sqlite_cur.fetchall()]

        for table in tables:
            print(f"Migrating table: {table}...")

            # Get column info
            sqlite_cur.execute(f"PRAGMA table_info({table});")
            columns = sqlite_cur.fetchall()
            
            # Translate schema (basic mapping)
            col_defs = []
            col_names = []
            for col in columns:
                # col: (id, name, type, notnull, dflt_value, pk)
                cid, name, ctype, notnull, dflt, pk = col
                pg_type = ctype.upper()
                name_l = name.lower()
                
                if "INTEGER" in pg_type and pk:
                    pg_type = "SERIAL PRIMARY KEY"
                elif "INTEGER" in pg_type:
                    pg_type = "INTEGER"
                elif "REAL" in pg_type or "DOUBLE" in pg_type or "NUMERIC" in pg_type:
                    pg_type = "NUMERIC"
                elif "TEXT" in pg_type:
                    if name_l == "date" or name_l.endswith("_date"):
                        pg_type = "DATE"
                    elif name_l == "created_at" or name_l == "generated_on" or pg_type == "DATETIME":
                        pg_type = "TIMESTAMP"
                    else:
                        pg_type = "TEXT"
                elif "DATETIME" in pg_type:
                    pg_type = "TIMESTAMP"
                
                # Check for other defaults or constraints if needed
                col_def = f'"{name}" {pg_type}'
                if not pk and notnull:
                    col_def += " NOT NULL"
                if dflt is not None:
                    # Basic default translation
                    d_upper = dflt.upper()
                    if d_upper == "CURRENT_TIMESTAMP" or "DATETIME('NOW')" in d_upper: 
                        dflt = "CURRENT_TIMESTAMP"
                    elif d_upper == "CURRENT_DATE" or "DATE('NOW')" in d_upper: 
                        dflt = "CURRENT_DATE"
                    elif d_upper.startswith("'") and d_upper.endswith("'"):
                        pass # String literal is fine
                    elif d_upper.replace('.','',1).isdigit():
                        pass # Number is fine
                    else:
                        # Might be a complex expression SQLite specific, default to NULL for safety or wrap
                        if "(" in dflt: dflt = "NULL" 
                        
                    if dflt != "NULL":
                        col_def += f" DEFAULT {dflt}"
                
                if not (pk and "SERIAL" in pg_type): # Serial already implies Primary Key
                    col_defs.append(col_def)
                else:
                    col_defs.append(col_def)

                col_names.append(name)

            # Drop table if exists and create
            pg_cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
            create_query = f'CREATE TABLE "{table}" ({", ".join(col_defs)});'
            print(f"Creating table in Postgres: {create_query}")
            pg_cur.execute(create_query)

            # Migrate data
            sqlite_cur.execute(f'SELECT * FROM "{table}";')
            rows = sqlite_cur.fetchall()
            if rows:
                placeholders = ", ".join(["%s"] * len(col_names))
                insert_query = f'INSERT INTO "{table}" ({", ".join([f"\"{n}\"" for n in col_names])}) VALUES ({placeholders})'
                pg_cur.executemany(insert_query, rows)
                print(f"Inserted {len(rows)} rows into {table}.")

            # Update sequences for SERIAL columns
            pg_cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_default LIKE 'nextval%';")
            seq_cols = pg_cur.fetchall()
            for (scol,) in seq_cols:
                pg_cur.execute(f"SELECT setval(pg_get_serial_sequence('\"{table}\"', '{scol}'), (SELECT MAX(\"{scol}\") FROM \"{table}\"));")

        pg_conn.commit()
        print("Migration completed successfully!")

    except Exception as e:
        print(f"Error during migration: {e}")
        if 'pg_conn' in locals(): pg_conn.rollback()
    finally:
        if 'sqlite_conn' in locals(): sqlite_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()

if __name__ == "__main__":
    if DB_CONFIG["dbname"] == "your_db_name":
        print("Error: Please update db_config.py with your PostgreSQL credentials first.")
        sys.exit(1)
    migrate()
