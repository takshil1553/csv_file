from sqlalchemy import create_engine, text
import csv
import pandas as pd  # Import the pandas library

# Database connection parameters
db_params = {
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432',
    'database': 'Fantasy_db'
}

# SQL query to select data from a table
select_query = text('SELECT * FROM public.match_type')

# SQL query to delete data from the table after export
delete_query = text('DELETE FROM public.match_type')

# Output CSV file name
output_csv = 'output.csv'

try:
    # SQLAlchemy database connection
    engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

    with engine.connect() as conn:
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

        print(f'Data exported to {output_csv}')

        # Create a pandas DataFrame from the CSV file
        df = pd.read_csv(output_csv)

        # Execute the SQL query to delete the data
        result = conn.execute(delete_query)

        # Commit the transaction
        conn.commit()

        print('Data deleted from the database')

except Exception as e:
    print('Error:', e)

# Now, you have a pandas DataFrame named 'df' containing the exported data from the CSV file.
# You can manipulate and analyze the data using pandas functions.
