import os
from dotenv import load_dotenv, find_dotenv
from rich import print
import pymysql
from pymysql.constants import CLIENT

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the MySQL server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the MySQL server
        self.connection = pymysql.connect(
            host=os.environ.get("HOST"),
            database=os.environ.get("MYSQL_DB"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS
        )

        # Create a cursor
        self.cursor = self.connection.cursor()

        # Check connection
        if self.verbose:
            self.cursor.execute("SELECT VERSION()")
            db_version = self.cursor.fetchone()
            print(f"Connected to: {db_version}")

    def close(self):
        self.cursor.close()
        self.connection.close()
        if self.verbose:
            print("Connection to MySQL database closed")
