import os
import psycopg2
from dotenv import load_dotenv, find_dotenv
from psycopg2.extras import execute_batch
from rich import print
from tqdm import tqdm
import math
from parser import pg_parse, read_data

from utils import time_this

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


def experiment():
    """
    This handles the Postgres version of the experiment.
    """
    data = read_data(5)
    data = pg_parse(data)
    reset_database()
    insert_data("trackpoint_no_index", data, 30)
    insert_data("trackpoint_indexed", data, 30)


def reset_database():
    """
    Reset all tables used in the Postgres version of the experiment.
    """
    db = Connector()
    with open("pg_schema.sql", "r") as file:
        db.cursor.execute(file.read())
    db.close()


@time_this
def insert_data(table: str, data: list, transaction_size: int):
    """
    Insert all supplied data into the specified table, in transactions with
    up to N rows per transaction.
    """
    db = Connector(verbose=False)
    query = f"""
    --sql
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_MakePoint(%s,%s), %s, %s, %s)
    ;
    """
    records = len(data)
    transactions = math.ceil(records / transaction_size)
    print(f"Inserting {records} rows over {transactions} transactions")
    print(f"Table: {table}")
    print(f"Batch size: {transaction_size} rows per transaction")
    print()

    for i in tqdm(range(0, records, transaction_size)):
        execute_batch(db.cursor, query, data[i : i + transaction_size], 1000)
        db.connection.commit()
    db.close()


if __name__ == "__main__":
    experiment()
