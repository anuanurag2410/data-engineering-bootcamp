Objectives 
What is Right Join?
How Right Join is Different From Left Join?
Why Right Join is Rarely used in the Real Life projects?
How Right Join can be rewritten as a LEFT Join?
Practical Examples and Questions 
Mistakes 


What is Right Join?
All the Records from the Right Table and the common records matching from the left table 
Simple meaning of Right join is --> Keep Everything fromt eh right table and bring the matching data from the left table.


-- Examples
-- I want the orders THat right joins the Customers 

USe DataX_Joins;
Select * from Orders as O 
RIGHT JOIN Customers as C
ON O.customer_id=C.Customer_id


Select * from Orders


-- LEFT JOIN VS RIGHT JOIN
Left Join Protects the left table 
Right join protects the right table 

-- LEFT JOIN
Select * from Customers as C
LEFT JOIN Orders as O 
ON C.customer_id=O.Customer_id

-- RIGHT JOIN
Select * from Customers as C
RIGHT JOIN Orders as O 
ON C.customer_id=O.Customer_id




-- LEFT JOIN
Select * from Customers as C
LEFT JOIN Orders as O 
ON C.customer_id=O.Customer_id


-- Converting Same Join to RIGHT JOIN
Select * from Orders as O 
RIGHT JOIN Customers as C
ON O.Customer_id=C.customer_id

-- Why Right Join is Rarely Used
1- Left Join is Easier to Read becuase developers read left to right 
2- Query Flow becomes more neutral
3- Developers usually will keep the Main/Base Table on the LEFT.
4- Any Right Join can be rewritten in the LEFT Join just by changing order of table.
5- Better for Code standards to use a left join as its easy to understand 


Select * from Customers;
Select * from Orders;
Select * from Payments;
Select * from Products;


Question 1 -- Show all the customers and their orders if avaliable
Select 
C.customer_id,
c.customer_name,
c.city,
o.order_id,
o.order_amount,
o.order_status
FROM Orders as O 
RIGHT JOIN
Customers as C 
ON O.Customer_id=C.customer_id


Question 2 -- Show all the customers even if they have not ordered anything

SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_amount
FROM orders o
RIGHT JOIN customers c
    ON o.customer_id = c.customer_id;


-- Lets do a LEFt Join Version 
SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_amount
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id;


-- Question 3 --> ALl products with the orders using RIght Join

SELECT
    p.product_id,
    p.product_name,
    p.category,
    o.order_id,
    o.order_amount
FROM orders o
RIGHT JOIN products p
    ON o.product_id = p.product_id;

INSERT INTO products
(product_id, product_name, category)
VALUES
(205, 'Webcam', 'Accessories');

Select * from Products

Select * from Payments
Select * from Orders

-- Question 4 --> I want all the orders with payments use RIGHT JOIN 
Select 
o.order_id,
o.order_amount,
o.order_status,
p.payment_id,
p.payment_status,
p.paid_amount
FROM Payments as p 
RIGHT JOIN Orders as o 
ON p.order_id=o.order_id

-- Question 5 --> Find the Customers who have never ordered UsE RIGHT JOIN 
SELECT
    c.customer_id,
    c.customer_name,
    c.city
FROM orders o
RIGHT JOIN customers c
    ON o.customer_id = c.customer_id
    WHERE o.order_id is NULL;

-- Question 6 --> Find the products never ordered USE RIGHT JOIN 
SELECT
    p.product_id,
    p.product_name,
    p.category,
    o.order_id,
    o.order_amount
FROM orders o
RIGHT JOIN products p
    ON o.product_id = p.product_id
    WHERE o.order_id is NULL;


-- Question 7 --> SHow Every Customer with Total Order Amount USE RIGHT JOIN
Select 
c.customer_id,
c.customer_name,
Coalesce(SUM(o.order_amount),0) AS Total_orders
FROM Orders as o 
RIGHT JOIN Customers as c 
ON o.customer_id=c.customer_id
GROUP BY c.customer_id,
c.customer_name








