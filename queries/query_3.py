from mysql.connector import Error
from appplication_context import cursor, conn
from utils.convert_table_to_csv import export

try:

    if conn.is_connected():
        cursor.execute("USE nse_stock_data;")
        cursor.execute("DROP TABLE IF EXISTS q3_tmp;")
        cursor.execute("CREATE TABLE q3_tmp AS SELECT * FROM stock_data_30_days GROUP BY symbol having timestamp = min(timestamp);")
        cursor.execute("DROP TABLE IF EXISTS q3_open;")
        cursor.execute("CREATE TABLE q3_open AS SELECT symbol, open FROM q3_tmp;")
        cursor.execute("DROP TABLE IF EXISTS q3_tmp;")
        cursor.execute("CREATE TABLE q3_tmp AS SELECT * FROM stock_data_30_days GROUP BY symbol having timestamp = max(timestamp);")
        cursor.execute("DROP TABLE IF EXISTS q3_close;")
        cursor.execute("CREATE TABLE q3_close AS SELECT symbol, close FROM q3_tmp;")
        cursor.execute("DROP TABLE IF EXISTS q3_tmp;")
        cursor.execute("CREATE TABLE q3_tmp AS SELECT * FROM q3_open NATURAL JOIN q3_close;")
        cursor.execute("DROP TABLE IF EXISTS q3_open;")
        cursor.execute("DROP TABLE IF EXISTS q3_close;")
        cursor.execute("CREATE TABLE query3_output AS SELECT * FROM q3_tmp ORDER BY ((close - open)/open) LIMIT 25;")
        cursor.execute("DROP TABLE IF EXISTS q3_tmp;")
        export('query3_output')


except Error as e:
    print(e)
