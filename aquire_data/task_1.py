import pandas as pd
import mysql.connector as c
from mysql.connector import Error
from dateutil import parser


securities_available_for_equity_segment = pd.read_csv('https://archives.nseindia.com/content/equities/EQUITY_L.csv')

try:
    conn = c.connect(host='localhost', user='root',
                        password='123456')#give ur username, password
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS nse_stock_data")

except Error as e:
    print("Error while connecting to MySQL", e)

try:
    conn = c.connect(host='localhost', database='nse_stock_data', user='root', password='123456')

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute('DROP TABLE IF EXISTS equity_segment;')
        cursor.execute("CREATE TABLE equity_segment(symbol varchar(255), company_name varchar(255), series varchar("
                       "20), listing_date date, paid_up_value int, market_lot int, isin_number varchar(255), "
                       "face_value "
                       "int);")

        for i, row in securities_available_for_equity_segment.iterrows():
            sql = "INSERT INTO nse_stock_data.equity_segment VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
            date = parser.parse(str(row[3]))
            data = (row[0], row[1], row[2], date, row[4], row[5], row[6], row[7])
            cursor.execute(sql, data)
            conn.commit()

    cursor = conn.cursor()


except Error as e:
    print("Error while connecting to MySQL", e)






