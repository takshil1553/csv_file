from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
import pandas as pd
from enum import Enum
import sqlalchemy as db
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid 

# Define the SQLAlchemy model
Base = declarative_base()

class MatchType(Base):
    __tablename__ = 'match_type'
    match_type_id = db.Column(UUID(as_uuid=True), primary_key=True,index=True,default=uuid.uuid4)
    match_type_name = db.Column(db.String,nullable=False)
    match_type_name_cricket_api = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime,default=datetime.now)
    updated_at = db.Column(db.DateTime,default=datetime.now)

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

    # Create an SQLAlchemy session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query all rows from the MatchType table
    match_types = session.query(MatchType).all()

    # Convert the MatchType objects to a list of dictionaries
    # data = [{'id': match.id, 'name': match.name} for match in match_types]
    data = [{'match_type_id': match.match_type_id, 'match_type_name': match.match_type_name, 'match_type_name_cricket_api': match.match_type_name_cricket_api, 'created_at': match.created_at, 'updated_at': match.updated_at} for match in match_types]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Export the DataFrame to a CSV file
    df.to_csv(output_csv, index=False)

    print(f'Data exported to {output_csv}')

    # Delete all rows from the MatchType table
    session.query(MatchType).delete()

    # Commit the transaction
    session.commit()

    print('Data deleted from the database')

except Exception as e:
    print('Error:', e)
