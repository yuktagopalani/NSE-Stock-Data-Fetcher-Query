import mysql.connector as c
from mysql.connector import Error
from utils.convert_table_to_csv import export
try:
    conn = c.connect(host='localhost', user='root',
                     password='123456')  # give ur username, password
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("USE nse_stock_data;")
        query = "SELECT * from latest_bhavcopy ORDER BY ((close_price - open_price)/open_price) DESC LIMIT 25;"
        cursor.execute("CREATE TABLE query1_output AS " + query)

    export('query1_output')
except Error as e:
    print(e)

