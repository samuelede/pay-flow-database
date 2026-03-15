-- DROP IF EXISTS
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- FINAL CUSTOMERS TABLE
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50),
    email VARCHAR(100)
);

-- FINAL MERCHANTS TABLE
CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    business_name VARCHAR(150),
    category VARCHAR(50),
    country VARCHAR(50)
);

-- FINAL TRANSACTIONS TABLE
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT,
    merchant_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    transaction_timestamp TIMESTAMP
);