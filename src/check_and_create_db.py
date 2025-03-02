import os
import psycopg2
from psycopg2 import sql
import subprocess

host = 'localhost'
user = 'lauraikic'
password = 'postgres'
database_name = 'city_bike_db'
dump_file_path = 'city_bike_db.dump'

def create_database_if_not_exists():
    try:
        connection = psycopg2.connect(
            host=host,
            database='postgres',
            user=user,
            password=password
        )
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [database_name])
            if cursor.fetchone():
                print(f"Database '{database_name}' already exists.")
            else:
                print(f"Database '{database_name}' does not exist. Creating it...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                print(f"Database '{database_name}' created.")

                if os.path.exists(dump_file_path):
                    print(f"Restoring database from dump file: {dump_file_path}")
                    subprocess.run(['psql', '-U', user, '-h', host, '-d', database_name, '-f', dump_file_path], check=True)
                    print(f"Database restored from dump file.")

        connection.close()

        connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password
        )
        with connection.cursor() as cursor:
            create_ride_table(cursor)

    except Exception as error:
        print(f"Error while creating the database: {error}")
    finally:
        if connection:
            connection.close()
            print("Connection closed.")

def create_database_if_not_exists():
    try:
        connection = psycopg2.connect(
            host=host,
            database='postgres',
            user=user,
            password=password
        )
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [database_name])
            if cursor.fetchone():
                print(f"Database '{database_name}' already exists.")
            else:
                print(f"Database '{database_name}' does not exist. Creating it...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                print(f"Database '{database_name}' created.")

                if os.path.exists(dump_file_path):
                    print(f"Restoring database from dump file: {dump_file_path}")
                    subprocess.run(['pg_restore', '-U', user, '-h', host, '-d', database_name, dump_file_path], check=True)
                    print(f"Database restored from dump file.")

        connection.close()

        connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password
        )
        with connection.cursor() as cursor:
            create_ride_table(cursor)

    except Exception as error:
        print(f"Error while creating the database: {error}")
    finally:
        if connection:
            connection.close()
            print("Connection closed.")

def create_ride_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ride (
        ride_id VARCHAR(255) PRIMARY KEY,
        rideable_type VARCHAR(50),
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        start_station_name VARCHAR(255),
        start_station_id VARCHAR(50),
        end_station_name VARCHAR(255),
        end_station_id VARCHAR(50),
        start_lat DOUBLE PRECISION,
        start_lng DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lng DOUBLE PRECISION,
        member_casual VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    print("Table 'ride' created (or already exists).")

def query_data():
    try:
        connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM ride LIMIT 10;")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

    except Exception as error:
        print(f"Error querying the database: {error}")

    finally:
        if connection:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    create_database_if_not_exists()
    query_data()