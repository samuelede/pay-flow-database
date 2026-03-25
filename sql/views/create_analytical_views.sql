-- =============================================
-- PayFlow - Analytical Views
-- Business Intelligence & Reporting Views
-- =============================================

-- 1. Merchant Performance View
CREATE OR REPLACE VIEW vw_merchant_revenue AS
SELECT 
    s.seller_id AS merchant_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_revenue,
    AVG(oi.price) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    MAX(o.order_purchase_timestamp) AS last_sale_date
FROM raw_sellers s
LEFT JOIN raw_order_items oi ON s.seller_id = oi.seller_id
LEFT JOIN raw_orders o ON oi.order_id = o.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_revenue DESC;


-- 2. Geographic Sales & Delivery Performance
CREATE OR REPLACE VIEW vw_geographic_performance AS
SELECT 
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_revenue,
    AVG(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) AS late_delivery_rate,
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) AS delivered_orders
FROM raw_customers c
JOIN raw_orders o ON c.customer_id = o.customer_id
JOIN raw_order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- 3. Payment Analysis
CREATE OR REPLACE VIEW vw_payment_trends AS
SELECT 
    p.payment_type,
    COUNT(*) AS transaction_count,
    SUM(p.payment_value) AS total_amount,
    AVG(p.payment_value) AS avg_transaction_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM raw_order_payments p
JOIN raw_orders o ON p.order_id = o.order_id
GROUP BY p.payment_type
ORDER BY total_amount DESC;


-- 4. Order Status Overview
CREATE OR REPLACE VIEW vw_order_status_summary AS
SELECT 
    order_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM raw_orders
GROUP BY order_status
ORDER BY order_count DESC;


-- 5. Customer Lifetime Value (Basic)
CREATE OR REPLACE VIEW vw_customer_ltv AS
SELECT 
    c.customer_unique_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_spent,
    MAX(o.order_purchase_timestamp) AS last_purchase,
    MIN(o.order_purchase_timestamp) AS first_purchase
FROM raw_customers c
JOIN raw_orders o ON c.customer_id = o.customer_id
JOIN raw_order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_unique_id
HAVING COUNT(DISTINCT o.order_id) >= 2
ORDER BY total_spent DESC;