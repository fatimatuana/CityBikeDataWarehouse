import os
import psycopg2
from psycopg2 import sql
import subprocess

host = 'localhost'
user = 'lauraikic'
password = 'postgres'
database_name = 'city_bike_db'
dump_file_path = 'city_bike_dump.sql'

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

    except Exception as error:
        print(f"Error while creating the database: {error}")
    finally:
        if connection:
            connection.close()
            print("Connection closed.")

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
