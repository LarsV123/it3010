import dataclasses
import sqlite3
import subprocess
import pg
import mysql_ex as my
import mongo
import random
from rich import print
from dataclasses import dataclass, field
from time import perf_counter
from parser import mongo_parse, mysql_parse, pg_parse, read_data
from utils import time_this

database = "experiments.db"
last_commit = subprocess.check_output(["git", "describe", "--always"]).strip().decode()


def initialize():
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    with open("experiment_log.sql") as file:
        cursor.executescript(file.read())
    print("Successfully initialized experiment log schema")


def table_exists(table: str) -> bool:
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"""
    --sql
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name='{table}'
    ;
    """
    cursor.execute(query)
    exists = bool(cursor.fetchall())
    connection.close()
    return exists


def get_row_count(table: str) -> int:
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"""
        --sql
        SELECT COUNT(*) FROM {table} 
        ;
        """
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    return row_count


@dataclass
class Setup:
    """
    Object which holds configuration and results from an experiment run.

    Usage:
    - Create the object and pass it all required parameters.
    - Then feed it experiment results afterwards and call save().
    """

    dbms: str
    table_name: str
    indexed: bool
    batch_size: int
    rows_inserted: int
    run_time: float = field(default=0, kw_only=True)
    git_hash: str = field(default=last_commit, kw_only=True)

    def validate(self):
        if self.run_time == 0:
            raise ValueError("Missing value: run_time")
        if self.rows_inserted == 0:
            raise ValueError("Missing value: rows_inserted")
        if self.batch_size < 1:
            raise ValueError(f"Invalid value: {self.batch_size=}")

    def save(self):
        self.validate()
        connection = sqlite3.connect(database)
        cursor = connection.cursor()

        query = f"""
        --sql
        INSERT INTO result
        (dbms, indexed, batch_size, rows_inserted, run_time, git_hash) 
        VALUES
        (:dbms, :indexed, :batch_size, :rows_inserted, :run_time, :git_hash)
        ;
        """

        values = dataclasses.asdict(self)
        cursor.execute(query, values)
        connection.commit()


def generate_experiments(row_count: int):
    """
    Generate all permutations of the experimental setup.
    """
    experiments: list[Setup] = []

    # Generate all permutations of the experimental setup
    for dbms in ["postgres", "mysql", "mongodb"]:
        for indexed in [True, False]:
            for batch_size in [2, 5, 10, 25, 50, 100, 200, 300, 500]:
                if indexed:
                    table = "trackpoint_indexed"
                else:
                    table = "trackpoint_no_index"

                setup = Setup(
                    dbms=dbms,
                    table_name=table,
                    indexed=indexed,
                    rows_inserted=row_count,
                    batch_size=batch_size,
                )
                experiments.append(setup)
    return experiments


@time_this
def run_experiments(row_count: int):
    """
    Run all possible permutations of the experimental setup.
    """
    raw_data = read_data(max_records=row_count)
    if len(raw_data) != row_count:
        raise ValueError(f"{len(raw_data)=} | {row_count=}")

    # Preprocess dataset for insertion
    pg_data = pg_parse(raw_data)
    mysql_data = mysql_parse(raw_data)
    mongo_data = mongo_parse(raw_data)

    experiments = generate_experiments(row_count)

    # Randomize order of experiments
    random.shuffle(experiments)

    for x in experiments:
        # Prepare DBMS-specific setup
        if x.dbms == "postgres":
            db = pg.Connector(verbose=False)
            db.reset_database()

            ## EXPERIMENT START
            start = perf_counter()
            pg.insert_data(db, x.table_name, pg_data, x.batch_size, row_count)
            end = perf_counter()
            # EXPERIMENT FINISHED

            db.connection.close()

        elif x.dbms == "mysql":
            db = my.Connector(verbose=False)
            db.reset_database()

            ## EXPERIMENT START
            start = perf_counter()
            my.insert_data(db, x.table_name, mysql_data, x.batch_size, row_count)
            end = perf_counter()
            # EXPERIMENT FINISHED

            db.connection.close()
        elif x.dbms == "mongodb":
            db = mongo.Connector(verbose=False)
            mongo.reset_database()

            ## EXPERIMENT START
            start = perf_counter()
            mongo.insert_data(db, x.table_name, mongo_data, x.batch_size, row_count)
            end = perf_counter()
            # EXPERIMENT FINISHED
        else:
            raise NotImplemented(f"Not implemented: {x.dbms}")

        # Clean-up phase
        elapsed = round(end - start, 2)
        x.run_time = elapsed
        x.rows_inserted = row_count
        x.save()
