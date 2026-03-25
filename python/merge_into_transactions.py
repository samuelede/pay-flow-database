"""
merge_into_transactions.py

- Executes create_transformed_tables.sql to build and populate analytical tables.
"""

import psycopg2
from pathlib import Path
from utils.db_config import DB_CONFIG

# Configuration
SQL_DIR = Path("sql")
TRANSFORM_SQL = SQL_DIR / "create_transformed_tables.sql"

def get_connection():
    """Create a new connection using config from db_config.py"""
    return psycopg2.connect(**DB_CONFIG) # type: ignore

def execute_transform_sql():
    """Reads and executes the transformation SQL script."""
    if not TRANSFORM_SQL.exists():
        print(f"! Transform SQL file not found: {TRANSFORM_SQL}")
        return

    print(f"→ Running transformation script: {TRANSFORM_SQL.name}")

    conn = None
    try:
        with open(TRANSFORM_SQL, "r", encoding="utf-8") as f:
            sql_content = f.read()

        conn = get_connection()
        # Using a context manager for the cursor ensures it closes automatically
        with conn.cursor() as cur:
            # Optional: Add logic here to clear old analytical data if needed
            # cur.execute("TRUNCATE TABLE transactions CASCADE;") 
            
            cur.execute(sql_content)
        
        conn.commit()
        print("   ✓ Transformation and population completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"   ✗ Transformation failed: {type(e).__name__}: {e}")
    finally:
        if conn:
            conn.close()

def main():
    print("=== PayFlow - Transformation & Analytical Tables ===\n")
    execute_transform_sql()
    print("\nTransformation pipeline finished.\n")

if __name__ == "__main__":
    main()