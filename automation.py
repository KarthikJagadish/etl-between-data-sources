# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
mysql_connection = mysql.connector.connect(user='root', password='password',host='host',database='database')
mysql_cursor = mysql_connection.cursor()

# Connect to DB2 or PostgreSql
dsn_hostname = 'hostname'
dsn_user='username'
dsn_pwd ='password'
dsn_port ="port"
dsn_database ="database" 

psql_conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

psql_cursor = psql_conn.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    psql_cursor.execute('SELECT rowid from sales_data order by timeestamp desc limit 1;')
    rows = psql_cursor.fetchall()
    psql_conn.commit()
    #psql_conn.close()
    for row in rows:
        num = row[0]
    return num


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    mysql_cursor.execute('SELECT * from sales_data where rowid > {}'.format(last_row_id))
    rows = mysql_cursor.fetchall()
    mysql_connection.commit()
    
    return rows

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    for row in records:
        SQL="INSERT INTO sales_data(rowid,product_id,customer_id,quantity) values(%s,%s,%s,%s)" 
        psql_cursor.execute(SQL,row)
        psql_conn.commit()
    pass

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
psql_conn.close()
# End of program
