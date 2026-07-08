WHAT IS JOIN 

The join is used to combine two or more tables togegher based on a common column.


Why Not to keep all the data in one single table?
Why to create multiple tables? 


Select * from Orders as O JOIN customer_events as C
ON O.customer_id=C.customer_id




-- Lest Create the Table
Create Database DataX_JOINS;
USE DataX_JOINS;


-- Create the Cusotmer Table 
CREATE TABLE IF NOT EXISTS Customers
(
customer_id INT PRIMARY KEY, 
customer_name VARCHAR(50), 
city VARCHAR(50)
);


-- Create the Orders Table 
CREATE TABLE IF NOT EXISTS Orders
(
order_id INT PRIMARY KEY, 
customer_id INT,
order_amount DECIMAL(10,2), 
order_date DATE
);

-- Insert the data to Cusotmers and Orders Table 

Insert Into Customers(Customer_id,Customer_name, City)
VALUES 
(1,'Anurag','Bengaluru'),
(2,'Stuti','Bengaluru'),
(3,'Vaishnav','Pune'),
(4,'Shruti','Pune'),
(5,'Shalini','Hyderabad')

Select * from Customers

-- Inserting to Orders table 
Insert Into Orders(Order_id,Customer_id,Order_amount, Order_date)
VALUES 
(101,1,50000,'2026-02-11'),
(102,2,87000,'2026-05-20'),
(103,1,70000,'2026-05-25'),
(105,3,508900,'2026-06-26'),
(106,3,578000,'2026-07-23'),
(107,3,66000,'2026-03-23')


Select *from orders 


-- Print the data that is present join order and customer table

Select 
c.customer_id,
o.order_id,
c.customer_name,
o.order_amount,
o.ORDER_DATE
FROM Orders as O 
JOIN Customers as C 
ON C.Customer_id=o.Customer_id



-- Where Joins can be used 

1- Reporting 
-- Requirement from business --: Show me the customer records customer_name, city, order, order amount , date 
Select 
c.customer_name,
c.city,
o.order_amount,
o.ORDER_DATE
FROM Orders as O 
JOIN Customers as C 
ON C.Customer_id=o.Customer_id



2--> Data Enrichment 
The process of gettign meaningful business logic information for the users from the transactional data and joining the 
master data is called as data enrichment 

3--> Data Validation 








Mistakes not to do as a begineer or in interviews 

1- Forgetting ON Conditions
Select * from customers Join orders 

-- We get wrong faulty data 

2- Joining on Wrong Column 





Types of Join we are going to discuss 

1- Inner join 
2- Left join 
3- Right Join 
4- Full outer Join 
5- Cross Join 
6- Self Join 
7- NON-EQUI Join 
8- Natural join 
9- Anti Join 
10- Semi Join 











