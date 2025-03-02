import os
import pandas as pd
from sqlalchemy import create_engine

def csv_to_postgres(csv_file, db_url):
    if not os.path.isfile(csv_file):
        print(f"Error: The file {csv_file} does not exist.")
        return

    df = pd.read_csv(csv_file, delimiter=';')

    engine = create_engine(db_url)

    df.to_sql('ride', engine, if_exists='append', index=False)

    print(f"Data from {csv_file} has been successfully inserted into the database.")

db_url = 'postgresql://lauraikic:postgres@localhost:5432/city_bike_db'

csv_file = 'city_bike_db.csv'

csv_to_postgres(csv_file, db_url)
