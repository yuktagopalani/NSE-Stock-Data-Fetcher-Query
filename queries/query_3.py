import mysql.connector as c
from mysql.connector import Error
from appplication_context import cursor, conn
try:

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("USE nse_stock_data;")
        query = "SELECT * FROM stock_data_30_days TAB1 GROUP BY SYMBOL ORDER BY (((SELECT CLOSE FROM stock_data_30_days WHERE TIMESTAMP=(SELECT MAX(TIMESTAMP) FROM stock_data_30_days  WHERE SYMBOL=TAB1.SYMBOL))-(SELECT OPEN FROM stock_data_30_days WHERE TIMESTAMP=(SELECT MIN(TIMESTAMP) FROM stock_data_30_days WHERE SYMBOL=TAB1.SYMBOL))/SELECT OPEN FROM stock_data_30_days WHERE TIMESTAMP=(SELECT MIN(TIMESTAMP) FROM stock_data_30_days WHERE SYMBOL=TAB1.SYMBOL) LIMIT 25;"
        cursor.execute("CREATE TABLE query3_output AS " + query)

except Error as e:
    print(e)