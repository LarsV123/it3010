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

        # Connect to the Mono server
        self.client = MongoClient(
            host=os.environ.get("HOST"),
            username=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
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

def experiment(record_number, batch_size, run_count, thread_count=1):
    conn = Connector(verbose=False)
    data = read_data(record_number=record_number)
    data = mongo_parse(data)
    for run in range(run_count):
        print(f"Run number: {run}")
        print("---------------------------------------")
        reset_database()
        insert_data(conn, "trackpoint_no_index", data, batch_size)
        insert_data(conn, "trackpoint_indexed", data, batch_size)
        print()
    conn.close()

@time_this
def insert_data(conn: Connector, collection_name, records: list, batch_size):
    records_number = len(records)
    batches = math.ceil(records_number / batch_size)
    print(f"Inserting {records_number} documents over {batches} batches")
    print(f"Collection: {collection_name}")
    print(f"Batch size: {batch_size} documents per batch")
    for i in tqdm(range(0, records_number, batch_size)):
        conn.db[collection_name].insert_many(records[i : i + batch_size])
