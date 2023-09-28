from sqlalchemy import create_engine, text
import csv

# Database connection parameters
db_params = {
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432',
    'database': 'Fantasy_db'
}

# Output CSV file directory
output_directory = '/home/admin-t/Desktop/csv file/'

# SQLAlchemy database connection
engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

# SQL query to select data from a table
select_query = text('SELECT * FROM public.match_type')

try:
    with engine.connect() as conn:
        # Loop to export and delete data multiple times
        for i in range(1, 6):  # Adjust the range as needed
            # SQL query to delete data from the table after export
            delete_query = text('DELETE * FROM public.match_type')  # Adjust the condition as needed

            # Output CSV file name
            output_csv = f'{output_directory}csvfile_{i}.csv'

            # Execute the SQL query to select data
            result = conn.execute(select_query)

            # Fetch all the data
            data = result.fetchall()

            # Get column names
            colnames = result.keys()

            # Open a CSV file for writing
            with open(output_csv, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)

                # Write column names as the header
                csv_writer.writerow(colnames)

                # Write data rows
                csv_writer.writerows(data)

            print(f'Data {i} exported to {output_csv}')

            # Execute the SQL query to delete the data
            conn.execute(delete_query, cond=i)
            print(f'Data {i} deleted from the database')

except Exception as e:
    print('Error:', e)
