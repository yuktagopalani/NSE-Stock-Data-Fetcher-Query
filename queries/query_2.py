import mysql.connector as c
from mysql.connector import Error
from aquire_data.task_4 import dates
from dateutil import parser

try:
    conn = c.connect(host='localhost', user='root',
                     password='123456')  # give ur username, password
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("USE nse_stock_data;")

        flag=0
        for date in dates:
            dt = str(parser.parse(str(date)))
            print(dt[:10])
            query = f"SELECT * from stock_data_30_days where timestamp="+ dt[:10] + " ORDER BY ((close - open)/open) DESC LIMIT 25;"
            if flag == 0:
                cursor.execute("CREATE TABLE query2_output AS " + query)
                flag = 1
            else:
                cursor.execute("INSERT INTO query2_output " + query)

except Error as e:
    print(e)
