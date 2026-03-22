"""
merge_into_transactions.py
Creates / refreshes enriched transactions fact table
"""

import psycopg2
from utils.db_config import DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def run_merge():
    conn = get_connection()
    cursor = conn.cursor()

    print("Creating / refreshing enriched transactions table...")

    merge_sql = """
    DROP TABLE IF EXISTS transactions;

    CREATE TABLE transactions AS
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        oi.product_id,
        oi.seller_id          AS merchant_id,
        oi.price,
        oi.freight_value,

        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,

        -- Some useful derived fields
        (oi.price + oi.freight_value) AS total_item_value,
        CASE 
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date 
            THEN true ELSE false 
        END AS is_late_delivery

    FROM raw_orders o
    INNER JOIN raw_order_items oi ON o.order_id = oi.order_id
    LEFT JOIN raw_order_payments p 
        ON o.order_id = p.order_id 
        AND p.payment_sequential = 1;   -- take primary payment only (simplification)
    """

    try:
        cursor.execute(merge_sql)
        conn.commit()
        print("✓ transactions table created/refreshed")

        # Optional: add indexes (very important for performance)
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON transactions(order_id);",
            "CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id ON transactions(merchant_id);",
            "CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);",
            "CREATE INDEX IF NOT EXISTS idx_transactions_purchase_timestamp ON transactions(order_purchase_timestamp);",
        ]

        for idx_sql in indexes:
            cursor.execute(idx_sql)
        conn.commit()
        print("✓ Indexes created")

        # Row count check
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        count = cursor.fetchone()[0]
        print(f"→ Final row count: {count:,}")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("=== Merging Olist data into transactions fact table ===\n")
    run_merge()
    print("\nDone.")