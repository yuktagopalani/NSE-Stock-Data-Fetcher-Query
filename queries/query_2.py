from mysql.connector import Error
from aquire_data.task_4 import dates
from dateutil import parser
from utils.convert_table_to_csv import export
from appplication_context import conn

try:
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("USE nse_stock_data;")
        cursor.execute("DROP TABLE IF EXISTS query2_output;")
        flag=0
        for date in dates:
            dt = str(parser.parse(str(date)))
            query = "CREATE TABLE query2_output_" + dt[:4] + dt[5:7] + dt[8:10] + " AS SELECT *, ((close - open)/open) as gain from stock_data_30_days where timestamp=\"" + dt[:10] + "\" ORDER BY gain DESC LIMIT 25;"

            table_name = "query2_output_" + dt[:4] + dt[5:7] + dt[8:10]

            cursor.execute("DROP TABLE IF EXISTS " + table_name + ";")
            cursor.execute(query)
            if flag == 0:
                cursor.execute("CREATE TABLE query2_output AS SELECT * FROM " + table_name + ";")
                flag = 1
            else:
                cursor.execute("INSERT INTO query2_output SELECT * FROM " + table_name + ";")
            cursor.execute("DROP TABLE " + table_name)

        cursor.execute("CREATE TABLE q2db LIKE query2_output;")
        cursor.execute("INSERT INTO q2db SELECT * FROM query2_output ORDER BY timestamp DESC;")
        cursor.execute("DROP TABLE query2_output")
        cursor.execute("RENAME TABLE q2db TO query2_output;")
        export('query2_output')

except Error as e:
    print(e)
