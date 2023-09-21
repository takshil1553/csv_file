import psycopg2
import csv


# Database connection parameters
db_params = {
    'dbname': 'Fantasy_db',
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432'
}

# SQL query to select data from a table
query = 'SELECT * FROM public.user'

# Output CSV file name
output_csv = '/home/admin-t/Desktop/csv file/user.csv'

try:
    # Establish a database connection
    conn = psycopg2.connect(**db_params)
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Execute the SQL query
    cursor.execute(query)
    
    # Fetch all the data
    data = cursor.fetchall()
    
    # Get column names
    colnames = [desc[0] for desc in cursor.description]
    
    # Open a CSV file for writing
    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # Write column names as the header
        csv_writer.writerow(colnames)
        
        # Write data rows
        csv_writer.writerows(data)
    
    print(f'Data exported to {output_csv}')
    
except psycopg2.Error as e:
    print('Error:', e)
    
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
