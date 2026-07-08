INNER JOIN -: 
Inner join says the matching records from the both tables 
or common record between the table as per the joinning key 



ON a.Customer_id=b.Customer_id

-- Creating Database for Practice 

CREATE DATABASE Joins_Practice;
Use Joins_Practice;

-- I am not Adding any foriegn Key willingy 


Customer Table 
Orders Table 
Product Table 
Payments Table 

-- DDL of Customers Table
Create Table If not Exists Customers 
(
customer_id Int Primary key ,
customer_name Varchar(50),
City Varchar(50), 
Signup_date DATE
);


-- DDL of Products Table
Create Table If not Exists Products 
(
product_id Int Primary key ,
product_name Varchar(50) NOT NULL,
category Varchar(50)
);



-- DDL of Orders Table
Create Table If not Exists Orders 
(
order_id Int Primary key ,
customer_id Int,
product_id Int,
order_amount decimal(10,2),
order_date DATE,
order_status Varchar(50)
);

-- DDL of Payments Table
Create Table If not Exists Payments 
(
Payment_id Int Primary key ,
order_id Int,
payment_mode Varchar(20),
payment_status Varchar(50),
paid_amount decimal(10,2),
payment_date DATE
);



-- Inserting Sample Data
INSERT INTO customers(customer_id,customer_name,city,signup_date)
VALUES
(1,'Anurag','Bengaluru','2026-06-29'),
(2,'Indresh','Pune','2026-06-27'),
(3,'Vaishnav','Hyderabad','2025-03-22'),
(4,'Shruti','Gwalior','2026-05-29'),
(5,'Pratyush','Gorakhpur','2026-06-30'),
(6,'Shalini','Bhubneshwar','2026-05-31'),
(7,'Anuj','Raulkela','2026-03-15'),
(8,'Stuti','Bengaluru','2026-02-06');



INSERT INTO products(product_id,product_name,category)
VALUES
(201, 'Laptop', 'Electronics'),
(202, 'Mouse', 'Accessories'),
(203, 'Keyboard', 'Accessories'),
(204, 'Monitor', 'Electronics');


INSERT INTO orders
(order_id, customer_id, product_id, order_amount, order_date, order_status)
VALUES
(101, 1, 201, 50000.00, '2026-06-01', 'Delivered'),
(102, 1, 202, 1000.00, '2026-06-02', 'Delivered'),
(103, 2, 203, 2500.00, '2026-06-03', 'Delivered'),
(104, 99, 204, 15000.00, '2026-06-04', 'Delivered'),
(105, 3, 999, 3500.00, '2026-06-05', 'Delivered'),
(106, NULL, 201, 45000.00, '2026-06-06', 'Pending'),
(107, 5, 202, 1200.00, '2026-06-07', 'Cancelled');




INSERT INTO payments
(payment_id, order_id, payment_mode, payment_status, paid_amount, payment_date)
VALUES
(1001, 101, 'UPI', 'Success', 50000.00, '2026-06-01'),
(1002, 102, 'Card', 'Success', 1000.00, '2026-06-02'),
(1003, 103, 'UPI', 'Failed', 0.00, '2026-06-03'),
(1004, 105, 'Wallet', 'Success', 3500.00, '2026-06-05'),
(1005, 999, 'UPI', 'Success', 3000.00, '2026-06-08');


select * from customers

select * from orders

select * from products

select * from payments


-- checkign counts of each 

select count(*) from customers  -- 8 

select count(*) from orders -- 7

select count(*)from products -- 4

select count(*)from payments -- 5


-- First query on inner join 

-- Question   SHow the valid orders with customer details 

Select 
O.order_id,
o.customer_id,
c.customer_name,
c.city,
o.order_amount, 
o.order_date,
o.order_status
FROM Orders as O 
INNER JOIN Customers as C
ON O.customer_id=C.customer_id


-- Question Show the order details iwth the custome name and product name 


Orders
Customers 
Porducts 


Select 
O.order_id,
o.customer_id,
p.product_id,
c.customer_name,
c.city,
o.order_amount, 
o.order_date,
o.order_status,
p.product_name
FROM Orders as O 
INNER JOIN Customers as C
ON O.customer_id=C.customer_id
INNER JOIN Products as P 
ON O.Product_id=P.Product_id


-- Show the cusotmer_name, customer details , order details , paymentmode and product details 
Select 
O.order_id,
o.customer_id,
p.product_id,
c.customer_name,
c.city,
o.order_amount, 
o.order_date,
o.order_status,
p.product_name,
pay.payment_mode,
Pay.payment_status
FROM Orders as O 
INNER JOIN Customers as C
ON O.customer_id=C.customer_id
INNER JOIN Products as P 
ON O.Product_id=P.Product_id
INNER JOIN Payments as Pay
ON Pay.order_id=o.order_id


-- Show only for Begaluru 

Select 
O.order_id,
o.customer_id,
p.product_id,
c.customer_name,
c.city,
o.order_amount, 
o.order_date,
o.order_status,
p.product_name,
pay.payment_mode,
Pay.payment_status
FROM Orders as O 
INNER JOIN Customers as C
ON O.customer_id=C.customer_id
INNER JOIN Products as P 
ON O.Product_id=P.Product_id
INNER JOIN Payments as Pay
ON Pay.order_id=o.order_id
Where C.City ='Bengaluru'


-- Aggregation Scenario 
-- Find the total revenue grenerated by each city 
SELECT 
c.city,
SUM(o.order_amount) as Total_Revenue
FROM customers c
JOIN Orders o ON
c.customer_id=o.customer_id 
GROUP BY c.city;













