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
    table_name: str = field(repr=False)
    indexed: bool
    batch_size: int
    experiment_size: int
    preload_size: int
    run_time: float = field(default=0, kw_only=True, repr=False)
    git_hash: str = field(default=last_commit, kw_only=True, repr=False)

    def validate(self):
        if self.run_time == 0:
            raise ValueError("Missing value: run_time")
        if self.experiment_size == 0:
            raise ValueError("Missing value: experiment_size")
        if self.preload_size == 0:
            raise ValueError("Missing value: preload_size")
        if self.batch_size < 1:
            raise ValueError(f"Invalid value: {self.batch_size=}")

    def save(self):
        self.validate()
        connection = sqlite3.connect(database)
        cursor = connection.cursor()

        query = f"""
        --sql
        INSERT INTO result (
            dbms,
            indexed,
            batch_size,
            experiment_size,
            preload_size,
            run_time,
            git_hash
        )
        VALUES (
            :dbms,
            :indexed,
            :batch_size,
            :experiment_size,
            :preload_size,
            :run_time,
            :git_hash
        );
        """

        values = dataclasses.asdict(self)
        cursor.execute(query, values)
        connection.commit()


def generate_experiments(experiment_size: int, preload_size: int):
    """
    Generate all permutations of the experimental setup.
    """
    experiments: list[Setup] = []

    # Generate all permutations of the experimental setup
    for dbms in ["postgres", "mysql", "mongodb"]:
        for indexed in [True, False]:
            for batch_size in [2, 5, 10, 25, 50, 100, 200, 300, 500, 1000, 1500, 2000]:
                if indexed:
                    table = "trackpoint_indexed"
                else:
                    table = "trackpoint_no_index"

                setup = Setup(
                    dbms=dbms,
                    table_name=table,
                    indexed=indexed,
                    experiment_size=experiment_size,
                    preload_size=preload_size,
                    batch_size=batch_size,
                )
                experiments.append(setup)
    return experiments


@time_this
def run_experiments(
    experiment_size: int,
    preload_size: int,
    max_iterations: int,
    current_iteration: int,
):
    """
    Run all possible permutations of the experimental setup.
    Each experiment is run with a clean slate, with the database completely
    reset. We then preload the database with a number of records to simulate
    real usage.

    Parameters:
    - experiment_size -- number of rows to insert during experiment
    - preload_size -- number of rows to insert before start of experiment
    """
    total_rows = experiment_size + preload_size
    raw_data = read_data(max_records=total_rows)
    if len(raw_data) != total_rows:
        raise ValueError(f"{len(raw_data)=} | {total_rows=}")
    preload_data = raw_data[:preload_size]
    experiment_data = raw_data[preload_size:]

    # Preprocess dataset for insertion
    pg_data = pg_parse(experiment_data)
    pg_preload = pg_parse(preload_data)
    mysql_data = mysql_parse(experiment_data)
    mysql_preload = mysql_parse(preload_data)
    mongo_data = mongo_parse(experiment_data)
    mongo_preload = mongo_parse(preload_data)

    experiments = generate_experiments(experiment_size, preload_size)

    # Randomize order of experiments
    random.shuffle(experiments)
    experiment_count = len(experiments)

    for index, x in enumerate(experiments):
        print()
        print(f"Iteration {current_iteration + 1}/{max_iterations}")
        print(f"Experiment {index + 1}/{experiment_count}")
        print(x)

        # Prepare DBMS-specific setup
        if x.dbms == "postgres":
            db = pg.Connector(verbose=False)
            db.reset_database()
            pg.insert(db, x.table_name, pg_preload, 25000, preload_size)
            db.close()

            ## EXPERIMENT START
            db = pg.Connector(verbose=False)
            start = perf_counter()
            pg.insert(db, x.table_name, pg_data, x.batch_size, experiment_size)
            end = perf_counter()
            db.close()
            # EXPERIMENT FINISHED

        elif x.dbms == "mysql":
            db = my.Connector(verbose=False)
            db.reset_database()
            my.insert(db, x.table_name, mysql_data, 25000, preload_size)
            db.close()

            ## EXPERIMENT START
            db = my.Connector(verbose=False)
            start = perf_counter()
            my.insert(db, x.table_name, mysql_preload, x.batch_size, experiment_size)
            end = perf_counter()
            db.close()
            # EXPERIMENT FINISHED

        elif x.dbms == "mongodb":
            db = mongo.Connector(verbose=False)
            mongo.reset_database()
            mongo.insert(db, x.table_name, mongo_preload, 25000, preload_size)
            db.close()

            ## EXPERIMENT START
            db = mongo.Connector(verbose=False)
            start = perf_counter()
            mongo.insert(db, x.table_name, mongo_data, x.batch_size, experiment_size)
            end = perf_counter()
            db.close()
            # EXPERIMENT FINISHED
        else:
            raise NotImplemented(f"Not implemented: {x.dbms}")

        # Clean-up phase
        elapsed = round(end - start, 2)
        print(f"Run time: {elapsed} seconds")
        x.run_time = elapsed
        x.rows_inserted = experiment_size
        x.save()
