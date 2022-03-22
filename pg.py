import os
import psycopg2
from dotenv import load_dotenv, find_dotenv
from psycopg2.extras import execute_batch
from rich import print
from tqdm import tqdm

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the Postgres server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the Postgres server
        self.connection = psycopg2.connect(
            host=os.environ.get("HOST"),
            database=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
        )

        # Create a cursor
        self.cursor = self.connection.cursor()

        # Check connection
        if self.verbose:
            self.cursor.execute("SELECT version()")
            db_version = self.cursor.fetchone()
            print(f"Connected to: {db_version[0]}")

    def close(self):
        self.cursor.close()
        self.connection.close()
        if self.verbose:
            print("Connection to Postgres database closed")

    def reset_database(self):
        """
        Reset all tables used in the Postgres version of the experiment.
        """
        with open("pg_schema.sql", "r") as file:
            self.cursor.execute(file.read())


def insert(db: Connector, table: str, data: list, batch_size: int, row_count: int):
    """
    Insert all supplied data into the specified table, in transactions of the
    given size.
    """
    query = f"""
    --sql
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_MakePoint(%s,%s), %s, %s, %s)
    ;
    """

    for i in tqdm(range(0, row_count, batch_size), leave=False):
        execute_batch(db.cursor, query, data[i : i + batch_size])
        db.connection.commit()
