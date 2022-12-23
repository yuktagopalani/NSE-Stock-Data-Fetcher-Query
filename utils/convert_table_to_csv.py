import mysql.connector


def fetch_table_data(table_name):

    cnx = mysql.connector.connect(
        host='localhost',
        database='nse_stock_data',
        user='root',
        password='123456'
    )

    cursor = cnx.cursor()
    cursor.execute('select * from ' + table_name)

    header = [row[0] for row in cursor.description]

    rows = cursor.fetchall()

    # Closing connection
    cnx.close()

    return header, rows


def export(table_name):
    header, rows = fetch_table_data(table_name)

    # Create csv file
    f = open(table_name + '.csv', 'w')

    # Write header
    f.write(','.join(header) + '\n')

    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')

    f.close()
    print(str(len(rows)) + ' rows written successfully to ' + f.name)



