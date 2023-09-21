import psycopg2
import json

# PostgreSQL database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'eazybusiness2',
    'user': 'postgres',
    'password': '**'
}

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
except psycopg2.Error as e:
    print("Error: Unable to connect to the database")
    print(e)
    exit()

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# SQL query to retrieve table names and column names
query = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = '**'
    ORDER BY table_name, ordinal_position;
"""

cursor.execute(query)

# Fetch all rows and convert them to a list of dictionaries
rows = cursor.fetchall()
result = {}

for row in rows:
    table_name, column_name = row
    if table_name not in result:
        result[table_name] = []
    result[table_name].append(column_name)

# Convert the result to JSON
json_result = json.dumps(result, indent=2)

# Print or use the JSON as needed
print(json_result)

# Close the database connection
cursor.close()
conn.close()
