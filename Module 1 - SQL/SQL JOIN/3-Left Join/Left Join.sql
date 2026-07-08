What is LEFT JOIN 
-> left join says All the records from the left table and the matching records from both table 


LEFT JOIN = INNER JOIN + ROW DATA FROM THE LEFT TABLE 

--Lets understand with Example 

USE Joins_practice ;

Select * from customers 
Select * from Orders 
Select count(*) from Payments
Select count(*) from Products  


-- Question 1 -: Show all the customers and their orders if avaliable 
Select 
c.customer_id,c.customer_name,o.order_amount,o.order_status
FROM Customers as c LEFT JOIN  Orders as O on c.customer_id=o.customer_id


-- Question 2 -: Show all the customers and their orders details if they have placed any order
Select 
c.customer_id,
c.customer_name,
o.order_id,
o.order_amount,
o.order_status
FROM Customers as c LEFT JOIN  Orders as O on c.customer_id=o.customer_id


# Questions 3 --> Find the Customers who have never ordered anything

Select 
c.customer_id,
c.customer_name,
c.city
FROM Customers as c LEFT JOIN  Orders as O on c.customer_id=o.customer_id 
Where o.order_id IS NULL 

# many times this is called as ANTI JOIN


# Questions 4 --> Find Orders with  Invalid Customer
SELECT 
c.customer_id,
c.customer_name,
o.order_status,
o.order_amount,
o.customer_id
FROM Orders as O  LEFT JOIN  Customers as c on o.customer_id=c.customer_id 
Where c.customer_id is NULL


# Questions 5 --> Find Orders with  Invalid Products

SELECT 
p.product_id,
p.product_name,
o.order_status,
o.order_amount,
o.product_id
FROM Orders as O  LEFT JOIN  Products as p on o.product_id=p.product_id 
Where p.product_id is NULL



# Questions 6 --> Find Payments with  Invalid Orders
Hint -: Find the payments where order id is not avaliable in the orders table 
# Homework 



# Questions 7 --> Find the orders without payments

SELECT 
o.order_id,
o.customer_id, 
o.order_amount,
o.order_status,
p.payment_id,
p.payment_status
FROM ORDERS as O 
LEFT JOIN PAYMENTS as P 
ON o.order_id=p.order_id
Where p.payment_id is NULL 








COALESCE 

--LEft Join with Coalesce 
Question -> I want all the customers with order status , if there is no orders there show "NO ORDERS"

Select 
c.customer_id,
c.customer_name,
COALESCE(o.order_amount, "0") as Order_Amount,
COALESCE(o.order_status,"NO ORDERS") as Order_Status
FROM Customers as c 
LEFT JOIN  
Orders as O 
ON c.customer_id=o.customer_id


-- LEFT JOIN WITH AGGREGATION 
-- Question -> Show every customer with total amount spent, if the customer has no order to show, then take the order amount as 0



Select 
c.customer_id,
c.customer_name,
COALESCE(AVG(o.order_amount),0) as TotalSpent
FROM Customers as c 
LEFT JOIN  
Orders as O 
ON c.customer_id=o.customer_id
Group by c.customer_id,
c.customer_name


# Compare Where and ON Clause in the LEFT JOIN 

-- THis is with Where Condition
SELECT 
c.customer_name,
o.order_id,
o.order_status
FROM Customers as c 
LEFT JOIN  
Orders as O 
ON c.customer_id=o.customer_id
WHERE o.order_status='DELIVERED'


-- This is with ON Condition
SELECT 
c.customer_name,
o.order_id,
o.order_status
FROM Customers as c 
LEFT JOIN  
Orders as O 
ON c.customer_id=o.customer_id
AND o.order_status='DELIVERED'


