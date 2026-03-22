# python/utils/db_config.py
import os
from pathlib import Path

# Option A: Use environment variables (recommended for security)
# export DB_HOST=localhost
# export DB_NAME=payflow_db
# export DB_USER=postgres
# export DB_PASSWORD=yourpassword
# export DB_PORT=5432

# Option B: Hard-coded (only for local dev – never commit!)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"), # replace with server name
    "database": os.getenv("DB_NAME", "payflow_db"), # replace with your exact db name
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "XXXXXXXX"), # replace with your password
    "port": os.getenv("DB_PORT", "5432"),
}

# If you prefer SQLAlchemy style later, you can also export a connection string:
# DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"