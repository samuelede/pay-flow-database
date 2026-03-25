# PayFlow Database - Data Dictionary

## Raw Tables

### `raw_customers`
- `customer_id` (TEXT, PK)
- `customer_unique_id` (TEXT)
- `customer_zip_code_prefix` (TEXT)
- `customer_city` (TEXT)
- `customer_state` (TEXT)

### `raw_orders`
- `order_id` (TEXT, PK)
- `customer_id` (TEXT)
- `order_status` (TEXT)
- `order_purchase_timestamp` (TIMESTAMP)
- `order_approved_at` (TIMESTAMP)
- `order_delivered_carrier_date` (TIMESTAMP)
- `order_delivered_customer_date` (TIMESTAMP)
- `order_estimated_delivery_date` (TIMESTAMP)

*(Continue for other tables...)*

## Transformed / Analytical Tables

### `transformed_transactions`
- `order_id`
- `customer_id`
- `merchant_id`
- `total_value`
- `payment_type`
- `is_late_delivery`
...