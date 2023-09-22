import psycopg2
from psycopg2 import sql

# Replace these with your PostgreSQL connection parameters
source_database_name = "eazybusiness"
source_user = "postgres"
source_password = "**"
source_host = "localhost"
source_port = "5432"

# Replace these with your destination PostgreSQL connection parameters (JTL database)
dest_database_name = "jtl"
dest_user = "postgres"
dest_password = "**"
dest_host = "localhost"
dest_port = "5432"
# Connect to the source PostgreSQL database
try:
    source_connection = psycopg2.connect(
        dbname=source_database_name,
        user=source_user,
        password=source_password,
        host=source_host,
        port=source_port
    )
except psycopg2.Error as e:
    print("Error connecting to the source database:", e)
    exit()

# Create a cursor object for the source database
source_cursor = source_connection.cursor()

# Define the SQL query to fetch column names, table names, and schema names
source_query = sql.SQL("""
    SELECT
        column_name,
        table_name,
        table_schema
    FROM information_schema.columns
    ORDER BY table_schema, table_name, ordinal_position
""")

# Execute the source query
try:
    source_cursor.execute(source_query)
    columns_info = source_cursor.fetchall()
except psycopg2.Error as e:
    print("Error executing the source query:", e)
    source_cursor.close()
    source_connection.close()
    exit()

# Connect to the destination PostgreSQL database (JTL)
try:
    dest_connection = psycopg2.connect(
        dbname=dest_database_name,
        user=dest_user,
        password=dest_password,
        host=dest_host,
        port=dest_port
    )
except psycopg2.Error as e:
    print("Error connecting to the destination database:", e)
    source_cursor.close()
    source_connection.close()
    exit()

# Create a cursor object for the destination database
dest_cursor = dest_connection.cursor()

# Define the SQL query to insert data into the Info_Tab table
insert_query = sql.SQL("""
    INSERT INTO info_column (id, column_name, table_name, schema_name)
    VALUES (%s, %s, %s, %s)
""")

# Insert the retrieved column information into the Info_Tab table in the destination database
for idx, column in enumerate(columns_info, start=1):
    data_to_insert = (idx, column[0], column[1], column[2])
    dest_cursor.execute(insert_query, data_to_insert)

# Commit the changes to the destination database
dest_connection.commit()
print("done")

# Close the cursors and database connections
source_cursor.close()
source_connection.close()
dest_cursor.close()
dest_connection.close()
