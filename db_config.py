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
        "port": result.port or 5432,
        "sslmode": "require"   # Required for hosted/cloud PostgreSQL
    }
else:
    # Local development fallback
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", os.getenv("DB_DATABASE", "milk_management")),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "1234"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432")
    }
