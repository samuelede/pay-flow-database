# PayFlow Database - Schema Diagram

```mermaid
erDiagram
    %% Raw Layer
    raw_customers {
        string customer_id PK
        string customer_unique_id
        string customer_zip_code_prefix
        string customer_city
        string customer_state
    }

    raw_sellers {
        string seller_id PK
        string seller_zip_code_prefix
        string seller_city
        string seller_state
    }

    raw_orders {
        string order_id PK
        string customer_id FK
        string order_status
        timestamp order_purchase_timestamp
        timestamp order_approved_at
        timestamp order_delivered_carrier_date
        timestamp order_delivered_customer_date
        timestamp order_estimated_delivery_date
    }

    raw_order_items {
        string order_id FK
        int order_item_id
        string product_id
        string seller_id FK
        numeric price
        numeric freight_value
    }

    raw_order_payments {
        string order_id FK
        int payment_sequential
        string payment_type
        int payment_installments
        numeric payment_value
    }

    %% Relationships
    raw_customers ||--o{ raw_orders : "places"
    raw_orders ||--o{ raw_order_items : "contains"
    raw_orders ||--o{ raw_order_payments : "has"
    raw_sellers ||--o{ raw_order_items : "sells"

    %% Analytical / Transformed Layer
    transformed_transactions {
        string order_id
        string customer_id
        string merchant_id
        timestamp purchase_timestamp
        numeric total_value
        string payment_type
        boolean is_late_delivery
    }

    %% Views (Analytical Layer)
    vw_merchant_revenue {
        string merchant_id
        string seller_city
        string seller_state
        int total_orders
        numeric total_revenue
        numeric avg_order_value
    }

    vw_geographic_performance {
        string customer_state
        int total_orders
        numeric total_revenue
        float late_delivery_rate
    }

    vw_payment_trends {
        string payment_type
        int transaction_count
        numeric total_amount
    }