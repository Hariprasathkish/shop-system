import os
import urllib.parse

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Render / Neon / Supabase provide a full connection URL
    # Format: postgresql://user:password@host:port/dbname
    result = urllib.parse.urlparse(DATABASE_URL)
    DB_CONFIG = {
        "dbname": result.path.lstrip("/"),
        "user": result.username,
        "password": result.password,
        "host": result.hostname,
        "port": result.port or 5432
    }
    
    # Render internal database hostnames (starting with 'dpg-') do NOT support SSL.
    # Other remote databases (like Neon/Supabase) require it.
    if result.hostname and not result.hostname.startswith("dpg-"):
        DB_CONFIG["sslmode"] = "require"
else:
    # Local development fallback
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", os.getenv("DB_DATABASE", "milk_management")),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "1234"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432")
    }
