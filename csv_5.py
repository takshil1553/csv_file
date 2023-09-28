from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import csv

# Define your SQLAlchemy model for the 'match_type' table
Base = declarative_base()

class MatchType(Base):
    __tablename__ = 'match_type'

    # id = Column('id', Integer, primary_key=True)
    # name = Column('name', String)
    # description = Column('description', Text)


# Database connection parameters
db_params = {
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432',
    'database': 'Fantasy_db'
}

# Output CSV file name
output_csv = 'output.csv'

try:
    # SQLAlchemy database connection
    engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query data from the MatchType table using ORM
    data = session.query(MatchType).all()
    
    colnames = session.query(MatchType).keys()

    # Open a CSV file for writing
    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header row
        csv_writer.writerow(colnames)

        # Write data rows
        for row in data:
            csv_writer.writerow(data)

    print(f'Data exported to {output_csv}')

    # Delete data from the MatchType table using ORM
    session.query(MatchType).delete()

    # Commit the transaction
    session.commit()

    print('Data deleted from the database')

except Exception as e:
    print('Error:', e)
