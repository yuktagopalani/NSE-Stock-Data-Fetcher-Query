import mysql.connector as c
from mysql.connector import Error
from dateutil import parser
from decouple import config


def insert_data_to_db(data):
    dates = []
    try:
        conn = c.connect(host='localhost', user='root',
                            password=config('mysql_pass'))#give ur username, password
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
            cursor.execute('DROP TABLE IF EXISTS stock_data_30_days;')
            cursor.execute("CREATE TABLE stock_data_30_days(symbol varchar(255), series varchar(20), open float(15), "
                           "high float(15), low float(15), close float(15), last float(15), tottrdqty int, "
                           "timestamp date);")

            for i, row in data.iterrows():
                sql = "INSERT INTO nse_stock_data.stock_data_30_days VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                date = parser.parse(str(row[8]))
                dates.append(date)
                try:
                    data = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], date)
                    cursor.execute(sql, data)
                except Error:
                    continue
                conn.commit()
            print('data inserted successfully ')

            dates = list(set(dates))

    except Error as e:
        print(e)

    print(dates)
    return dates