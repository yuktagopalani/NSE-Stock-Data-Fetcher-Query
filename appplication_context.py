import os
import mysql.connector as c
from mysql.connector import Error
from decouple import config

conn = c.connect(host='localhost', user='root',
                     password=config('mysql_pass'))  # give ur username, password
if conn.is_connected():
    cursor = conn.cursor()
# except Error as e:
#     print("Error while connecting to MySQL", e)
