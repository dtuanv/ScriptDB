import psycopg2
import json

# PostgreSQL database connection parameters
db_params = {
       'host': 'localhost',
    'database': 'eazybusiness2',
    'user': 'postgres',
    'password': '**'
}

# Specify the absolute path to the output JSON file
output_file_path = 'T:\Project\JTL/jtldb.json'

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
except psycopg2.Error as e:
    print("Error: Unable to connect to the database")
    print(e)
    exit()

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# SQL query to retrieve table names and column names from all schemas
query = """
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name, ordinal_position;
"""

cursor.execute(query)

# Fetch all rows and convert them to a list of dictionaries
rows = cursor.fetchall()
result = {}

for row in rows:
    schema, table_name, column_name = row
    if schema not in result:
        result[schema] = {}
    if table_name not in result[schema]:
        result[schema][table_name] = []
    result[schema][table_name].append(column_name)

# Convert the result to JSON
json_result = json.dumps(result, indent=2)

# Write the JSON content to the output file
with open(output_file_path, 'w') as output_file:
    output_file.write(json_result)

# Close the database connection
cursor.close()
conn.close()

# Print a message indicating where the JSON was saved
print(f"JSON data saved to: {output_file_path}")
