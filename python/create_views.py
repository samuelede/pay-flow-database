"""
create_views.py
Creates analytical views for business intelligence and reporting.
Consistent with your other psycopg2 scripts.
"""

from pathlib import Path
import psycopg2
from utils.db_config import DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG) # type: ignore


def create_analytical_views():
    views_file = Path("sql/views/create_analytical_views.sql")
    
    if not views_file.exists():
        print(f"Views file not found: {views_file}")
        return False

    print("→ Creating Analytical Views...")

    conn = None  # ← Important: declare conn outside try block

    try:
        with open(views_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(sql_content)
        conn.commit()

        print("   ✓ All analytical views created successfully!")
        return True

    except Exception as e:
        print(f"   ✗ Failed to create views: {type(e).__name__}: {e}")
        
        if conn:                    # Safe check before rollback
            try:
                conn.rollback()
            except:
                pass                # Ignore errors during rollback
        return False

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_analytical_views()