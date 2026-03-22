TRUNCATE TABLE customers, merchants, transactions CASCADE; 

INSERT INTO customers (customer_id, name, country, email)
SELECT
row_number() over(ORDER BY c.customer_id) as customer_id, 
c.customer_id as name,
'Brazil' as country,
concat(c.customer_id, '@customer.payflow.com') as email
from (select distinct customer_id from raw_customers) c;

INSERT INTO merchants (merchant_id, business_name, category, country)
SELECT
row_number() over(ORDER BY s.seller_id) as merchant_id, 
s.seller_id as business_name,
'ecommerce' as category,
'Brazil' as country
from (select distinct seller_id from raw_sellers) s;

INSERT INTO transactions (customer_id, merchant_id, amount, status, transaction_timestamp)
SELECT 
    
    DENSE_RANK() OVER (ORDER BY ro.customer_id) as customer_id,
    DENSE_RANK() OVER (ORDER BY roi.seller_id) as merchant_id,
    
    roi.price as amount,
    
    ro.order_status as status,
    
    ro.order_purchase_timestamp as transaction_timestamp
    
FROM raw_orders ro

INNER JOIN raw_order_items roi ON ro.order_id = roi.order_id

LEFT JOIN raw_order_payments rop ON ro.order_id = rop.order_id

WHERE rop.payment_sequential = 1 OR rop.payment_sequential IS NULL

ORDER BY ro.order_purchase_timestamp, ro.order_id, roi.order_item_sequence;
	