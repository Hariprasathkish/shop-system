import os

# Get the full connection string
DATABASE_URL = os.getenv("DATABASE_URL")

# Fallback configuration for local development
LOCAL_DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", os.getenv("DB_DATABASE", "milk_management")),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

