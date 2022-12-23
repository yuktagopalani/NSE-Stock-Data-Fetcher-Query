import pandas as pd
from dateutil import parser

from mysql.connector import Error

from appplication_context import cursor, conn
latest_bhavcopy = pd.read_csv('https://archives.nseindia.com/products/content/sec_bhavdata_full_22122022.csv')


cursor.execute("CREATE DATABASE IF NOT EXISTS nse_stock_data")
print("Database is created")


try:
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute('DROP TABLE IF EXISTS latest_bhavcopy;')
        cursor.execute("CREATE TABLE latest_bhavcopy(symbol varchar(255), series varchar(20), "
                       "date1 date, prev_close "
                       "float(15), open_price float(15), high_price float(15), low_price float(15), last_price float("
                       "15), close_price float(15), avg_price float(15), ttl_trd_q_qnty int, turnover_lacs float(15),"
                       "no_of_trades int, deliv_qty int, deliv_per float(15));")

        for i, row in latest_bhavcopy.iterrows():
            sql = "INSERT INTO nse_stock_data.latest_bhavcopy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            date = parser.parse(str(row[2]))
            deliv_qty = row[13]
            deliv_per = row[14]
            last_price = row[7]
            if deliv_qty == ' -':
                deliv_qty = None
            if deliv_per == ' -':
                deliv_per = None
            try:
                data = (row[0], row[1], date, row[3], row[4], row[5], row[6], last_price, row[8], row[9], row[10], row[11],
                        row[12], deliv_qty, deliv_per)
                cursor.execute(sql, data)
            except Error as e:
                continue
            conn.commit()

    cursor = conn.cursor()


except Error as e:
    print("Error while connecting to MySQL", e)
