"""
load_raw_data.py

Loads Olist CSV files into raw PostgreSQL tables.
Handles NULLs correctly in timestamp columns.
"""

import pandas as pd
from pathlib import Path
import psycopg2
from io import StringIO
import csv
from utils.db_config import DB_CONFIG

# Configuration
SQL_DIR = Path("sql")
DATA_DIR = Path("dataset")
RAW_SCHEMA_SQL = SQL_DIR / "create_raw_tables.sql"

TABLE_MAPPING = {
    "olist_customers_dataset.csv":       "raw_customers",
    "olist_sellers_dataset.csv":         "raw_sellers",
    "olist_orders_dataset.csv":          "raw_orders",
    "olist_order_items_dataset.csv":     "raw_order_items",
    "olist_order_payments_dataset.csv":  "raw_order_payments",
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG) # type: ignore

def execute_schema_sql():
    if not RAW_SCHEMA_SQL.exists():
        print(f"Skipping schema: {RAW_SCHEMA_SQL} not found.")
        return

    conn = None
    try:
        with open(RAW_SCHEMA_SQL, "r", encoding="utf-8") as f:
            sql = f.read()
        
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"✓ Schema executed: {RAW_SCHEMA_SQL.name}")
    except Exception as e:
        if conn: conn.rollback()
        print(f"✗ Schema error: {e}")
    finally:
        if conn: conn.close()

def load_csv_to_table(csv_path: Path, table_name: str):
    if not csv_path.exists():
        print(f"! File missing: {csv_path.name}")
        return

    conn = None
    try:
        # Load CSV - keeping it as strings initially prevents 'NaN' float conversion issues
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
        
        # Standardize Nulls: Replace all variants with None (Python's native NULL)
        df = df.replace(['', 'nan', 'NaN', 'null', 'NULL', '\\N'], None)

        # Buffer for COPY
        buffer = StringIO()
        # Use sep='\t' and na_rep='\\N' for the most reliable PostgreSQL COPY performance
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', quoting=csv.QUOTE_NONE)
        buffer.seek(0)

        conn = get_connection()
        with conn.cursor() as cur:
            # truncate table to prevent duplicates if re-running
            cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
            
            # Use copy_from for better performance and simplicity
            cur.copy_from(buffer, table_name, sep='\t', null='\\N')
            
        conn.commit()
        print(f"✓ {table_name}: Loaded {len(df):,} rows.")

    except Exception as e:
        if conn: conn.rollback()
        print(f"✗ {table_name} failed: {e}")
    finally:
        if conn: conn.close()

def main():
    print("--- Starting Pipeline ---")
    execute_schema_sql()
    
    for filename, table_name in TABLE_MAPPING.items():
        load_csv_to_table(DATA_DIR / filename, table_name)
    
    print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()