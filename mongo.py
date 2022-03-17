import os
import math
from dotenv import load_dotenv, find_dotenv
from rich import print
from pymongo import MongoClient, GEOSPHERE
from tqdm import tqdm

from parser import mongo_parse, read_data
from utils import time_this

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the MongoDB server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the Mongo server
        self.client = MongoClient(
            host=os.environ.get("HOST"),
            username=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
        )

        self.db = self.client.get_database(name=os.environ.get("MONGO_DB"))

        # Check connection
        if self.verbose:
            server_info = self.client.server_info()
            print(f"Connected to: {server_info}")

    def close(self):
        self.client.close()
        if self.verbose:
            print("Connection to MongoDB database closed")


def reset_database():
    """
    Reset all indexes and collection used in the MongoDB version of the experiment.
    """
    conn = Connector(verbose=False)
    db = conn.db
    db.drop_collection("trackpoint_no_index")
    db.drop_collection("trackpoint_indexed")
    db.create_collection("trackpoint_no_index")
    db.create_collection("trackpoint_indexed")
    db["trackpoint_indexed"].create_index([("tp_point", GEOSPHERE)])
    conn.close()


def insert_data(
    conn: Connector,
    collection_name: str,
    records: list,
    batch_size: int,
    row_count: int,
):
    for i in tqdm(range(0, row_count, batch_size)):
        conn.db[collection_name].insert_many(records[i : i + batch_size])
