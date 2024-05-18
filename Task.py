import mysql.connector
import pandas as pd

host = "localhost"
port = "3306"
uname = "root"
pwd = "Dhanya@2004"
db = "saferteck_backend"

#Connect
try:
    db_connect = mysql.connector.connect(
        host=host,
        port=port,
        user=uname,
        password=pwd,
        database=db
    )
    print("Connected to database")
except Exception as e:
    print(e)


def execute_query(query):
    db_cursor = db_connect.cursor(dictionary=True)
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    db_cursor.close()
    return pd.DataFrame(result)


def q1():
    query = "select * from Customers"
    return execute_query(query)

def q2():
    query = "select * from Orders where DATE_FORMAT(OrderDate, '%Y-%m') = '2023-01'"
    return execute_query(query)

def q3():
    query = """
    select o.OrderID, o.OrderDate, c.FirstName, c.LastName, c.Email
    from Orders o
    join Customers c on o.CustomerID = c.CustomerID
    """
    return execute_query(query)

def q4(order_id):
    query = f"""
    select p.ProductName, o.Quantity
    from OrderItems o
    join Products p on o.ProductID = p.ProductID
    where o.OrderID = {order_id}
    """
    return execute_query(query)

def q5():
    query = """
    select c.CustomerID, SUM(p.Price * i.Quantity) AS TotalSpent
    from Customers c
    join Orders o on c.CustomerID = o.CustomerID
    join OrderItems i on o.OrderID = i.OrderID
    join Products p on i.ProductID = p.ProductID
    group by c.CustomerID
    """
    return execute_query(query)


def q6():
    query = """
    select o.ProductID, p.ProductName,sum(o.Quantity) AS TotalOrdered
    from OrderItems o
    join Products p on o.ProductID = p.ProductID
    group by  o.ProductID,p.ProductName
    order by TotalOrdered desc limit 1
    """
    return execute_query(query)


def q7():
    query = """
    select DATE_FORMAT(OrderDate, '%Y-%m') AS Month, 
           count(distinct o.OrderID) AS TotalOrders,
           sum(p.Price * i.Quantity) AS TotalSales
    from Orders o
    join OrderItems i on o.OrderID = i.OrderID
    join Products p on i.ProductID = p.ProductID
    where DATE_FORMAT(OrderDate, '%Y') = '2023'
    group by DATE_FORMAT(OrderDate, '%Y-%m')
    """
    return execute_query(query)


def q8():
    query = """
    select c.CustomerID, c.FirstName,SUM(p.Price * i.Quantity) AS TotalSpent
    from Orders o
    join Customers c on c.CustomerID = o.CustomerID
    join OrderItems i on o.OrderID = i.OrderID
    join Products p on i.ProductID = p.ProductID
    group by c.CustomerID,c.FirstName
    having TotalSpent > 1000
    """
    return execute_query(query)


if __name__ == "__main__":

    print("\nList all customers")
    print(q1())

    print("\nFind all orders placed in January 2023")
    print(q2())

    print("\nGet the details of each order, including the customer name and email")
    print(q3())

    print("\nList the products purchased in a specific order ID 1")
    print(q4(1))


    print("\nCalculate the total amount spent by each customer")
    print(q5())

    print("\nFind the most popular product ")
    print(q6())

    print("\nGet the total number of orders and the total sales amount for each month in 2023")
    print(q7())

    print("\nFind customers who have spent more than $1000")
    print(q8())

db_connect.close()