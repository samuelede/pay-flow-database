CREATE TABLE raw_customers (
customer_id varchar(50),
customer_unique_id varchar(50),
customer_zip_code_prefix varchar(10),
customer_city varchar(100),
customer_state varchar(2));

-- RAW SELLERS TABLE
CREATE TABLE raw_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

-- RAW ORDERS TABLE
CREATE TABLE raw_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- RAW ORDER ITEMS TABLE
CREATE TABLE raw_order_items (
    order_id VARCHAR(50),
    order_item_sequence INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);

-- RAW ORDER PAYMENTS TABLE
CREATE TABLE raw_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2)
);