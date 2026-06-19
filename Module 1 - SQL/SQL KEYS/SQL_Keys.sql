Here the Keys will be helpful in Real world scenario-: 
Table Design 
Data Modeling 
Joins 
Data Quality Checks
Duplicate Detections 
Primary Key and Foriegn Key Relationships
Bulding The fact and Dimention Tables 
Source to Target Validation



PRIMARY KEY 
PRimary key is a column in the table or it can be group of columns that uniquely identifies each row in the table. 

Primary key has two very important properties-: 
1- It cannot be Null  
2- It has to be Unique 


-- DDL fo the Customer Table 
CREATE TABLE IF NOT EXISTS Customers
(
customer_id INT PRIMARY KEY, 
customer_name VARCHAR(50),
email VARCHAR(50),
phone VARCHAR(50),
city VARCHAR(50)
);


INSERT INTO CUSTOMERS
VALUES(103,'Anurag','anurag.srivastava2@gmail.com','798956658','Hyderabad'),
(102,'Anurag','anurag.srivastava1@gmail.com','798958858','Mumbai')



Select * from CUSTOMERS


-- Entring Duplicate to check primary key voilations
INSERT INTO CUSTOMERS
VALUES(102,'Anurag','anurag.srivastava2@gmail.com','798956658','Hyderabad')


INSERT INTO CUSTOMERS
VALUES(NULL,'Anurag','anurag.srivastava2@gmail.com','798956658','Hyderabad')


-- Uniquely identifying using cusotmer id 
Select * from CUSTOMERS where customer_id=101


2- Foreign Key 
A foreign Key is a column in one table refers PRIMARY KEY of another table


-- DDL of the Orders Table 

CREATE TABLE IF NOT EXISTS Orders
(
order_id INT PRIMARY KEY, 
customer_id INT, 
order_date DATE, 
total_amount DECIMAL(10,2),

CONSTRAINT fk_orders_customers
FOREIGN KEY (customer_id)
REFERENCES Customers(customer_id)
);

-- Inserting Values for Valid Cutomers
INSERT INTO Orders VALUES 
(1,101,'2025-10-06',600.00),
(2,101,'2025-10-06',600.00)

-- Invalid Cusomer Orders ENtry 
INSERT INTO Orders VALUES 
(3,106,'2026-10-06',6000.00),
(4,107,'2025-10-06',900.00)
-- YOu will get the error of Referencial Integrity



-- Now Entrying the not avlaialbe valies of customer id to the Cusotmer table
INSERT INTO CUSTOMERS
VALUES(106,'Shruti','Shruti.ss@gmail.com','798956658','Pune'),
(107,'Stuti','Stuti.ojha@gmail.com','798956658','Gorakhpur')

Select * from CUSTOMERS

-- Trying to run the same insert commaand that we got the error in orders table of referencial integrity
INSERT INTO Orders VALUES 
(3,106,'2026-10-06',6000.00),
(4,107,'2025-10-06',900.00)

Select * from Orders


-- Deleting Records that customer id is not htere in orders table 
DELETE FROM CUSTOMERS WHERE Customer_id =103
-- Successfully deleted

Select * from CUSTOMERS

-- Now deleting that value that is used in orders table 
DELETE FROM CUSTOMERS WHERE Customer_id =101
-- Error of Refrencial Integrity

Error Code: 1451. Cannot delete or update a parent row: 
a foreign key constraint fails (`sqlkeys`.`orders`, CONSTRAINT `fk_orders_customers` FOREIGN KEY (`customer_id`) 
REFERENCES `customers` (`customer_id`))


-- Join The tables using PRIMARY AND FOREIGN KEY 
-- I want the cusomter and the order details of the customer

SELECT
c.customer_id,
c.customer_name,
o.order_id,
o.order_date,
o.total_amount
FROM CUSTOMERS as c
JOIN ORDERS as o 
ON c.customer_id=o.customer_id


-- This is how we define the relationship between two tables using primary and foriegn key


3- Candidate Key 
Candidate key is any column or a combination or group of columns that can uniquely identify the row.
Table can have multiple Canditdate keys
From Those candidate key you can choose any one primary key 


Select * from Customers 

Here in this we can have possbile candidate keys 
- customer_id
- email
- Phone 

-- NOTE : All the Primary key will be candidate key but all the candidate key cannot be a primary keys 

4- Alternate Key 
An Alternate key is the key that is not selected as the primary key 

So in our case as we selected the Cusotmer_id as PRIMARY from the group of candidate key 

now 
- email
-Phone are the alternate key 

CREATE TABLE IF NOT EXISTS Customers
(
customer_id INT PRIMARY KEY, 
customer_name VARCHAR(50),
email VARCHAR(50) UNIQUE,
phone VARCHAR(50) UNIQUE,
city VARCHAR(50)
);



5- Unique Key 

CREATE TABLE IF NOT EXISTS Customers_Unique_Test
(
customer_id INT PRIMARY KEY, 
customer_name VARCHAR(50),
email VARCHAR(50) UNIQUE,
phone VARCHAR(50) UNIQUE,
city VARCHAR(50)
);

INSERT INTO Customers_Unique_Test
VALUES(106,'Shruti','Shruti.ss@gmail.com','798956658','Pune'),
(107,'Stuti','Stuti.ojha@gmail.com','79899956658','Gorakhpur')

Select * from Customers_Unique_Test

INSERT INTO Customers_Unique_Test
VALUES(1208,'Shruti','Shruti.ssaaa@gmail.com',NULL,'Pune'),
(NULL,'Stuti','Stuti.ojaahaaa@gmail.com',NULL,'Gorakhpur')

Error Code: 1062. Duplicate entry 'Shruti.ss@gmail.com' for key 'customers_unique_test.email'
Error Code: 1062. Duplicate entry '798956658' for key 'customers_unique_test.phone'

---------
6- Super Key 
A super key is any column or group or combimnation of columns that uniquely identfy the row 
it may contain extra column also 

Select * from Customers

Super Keys in this Table 
- customer_id 
- email
- phone
- customer_id + customer_name 
 - customer_id + city
 - email + city



------------------


7 - Composite key 

 CREATE TABLE IF NOT EXISTS Customers_Composite_Test
(
customer_id INT , 
customer_name VARCHAR(50),
email VARCHAR(50) UNIQUE,
phone VARCHAR(50) UNIQUE,
city VARCHAR(50),
PRIMARY KEY (email,phone)
);



8- Natural Key 
Natural key is a real world business column that can uniquely identify the record 

Real World Business column Example 
ORDERCODE UNIQUE
PH10
PH01 
TR01
TR02 

9- Surrogate Key 
Surrogate key is an artificial Key created by the database or system 


CREATE TABLE IF NOT EXISTS Customers_Surrogate_Test
(
customer_id INT AUTO_INCREMENT PRIMARY KEY, 
customer_name VARCHAR(50),
email VARCHAR(50) UNIQUE,
phone VARCHAR(50) UNIQUE,
city VARCHAR(50)
);


INSERT INTO Customers_Surrogate_Test
(customer_name,email,phone,city)
VALUES('Shruti','Shruti.ssaaaaa@gmail.com',NULL,'Pune'),
('Stuti','Stuti.ojaahaaaaa@gmail.com',NULL,'Gorakhpur')

Select * from Customers_Surrogate_Test

10- Secondary key 
A secondary key is not always the formal foreign or primary key, iot is the columns or column that we use frequently for the 
index, filter or search 



NOT NULL vs KEY 

CUSOTMER_NAME VARCHAR(50) NOT NULL 
EMAIL VARCHAR(50) NOT NULL 


NOT NULL 
it cannot have null values 

UNIQUE 
it cannot have duplicate values but can have nulls 

PRIMARY KEY
it cannot be null or it cannot be repeated 






