DROP DATABASE IF EXISTS sql_execution_order;

CREATE DATABASE sql_execution_order;

USE sql_execution_order;

-- CUSTOMER TABLE 
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    city VARCHAR(50)
);

INSERT INTO customers
(customer_id, customer_name, city)
VALUES
(1, 'Anurag', 'Bengaluru'),
(2, 'Stuti', 'Pune'),
(3, 'Shalini', 'Delhi'),
(4, 'Shruti', 'Mumbai'),
(5, 'Vaishnav', 'Bengaluru');


-- orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_category VARCHAR(50),
    order_amount DECIMAL(10,2),
    order_status VARCHAR(30),
    order_date DATE
);

INSERT INTO orders
(order_id, customer_id, product_category, order_amount, order_status, order_date)
VALUES
(101, 1, 'Electronics', 50000.00, 'Delivered', '2026-06-01'),
(102, 1, 'Accessories', 1000.00, 'Delivered', '2026-06-02'),
(103, 2, 'Accessories', 2500.00, 'Delivered', '2026-06-03'),
(104, 2, 'Electronics', 48000.00, 'Delivered', '2026-06-04'),
(105, 3, 'Accessories', 3500.00, 'Cancelled', '2026-06-05'),
(106, 4, 'Electronics', 15000.00, 'Pending', '2026-06-06'),
(107, 5, 'Accessories', 1200.00, 'Delivered', '2026-06-07'),
(108, 5, 'Electronics', 22000.00, 'Delivered', '2026-06-08');


Select  * from Customers;
Select * from orders;

-- Business Scenario 
-- Find cities where delivered order revenue is more than 50,000.
-- Show the city and total revenue, highest revenue first.








