import pg
import mysql
import mongo
import click
from rich import print


@click.group()
def cli():
    pass


@cli.command()
@click.argument("dbms", type=str)
def init(dbms: str):
    """
    Run script to initialize the experiment schema for the specified DBMS.
    """
    print(f"Initializing experiment table for: {dbms=}")
    if dbms == "pg":
        pg.reset_database()
    elif dbms == "mysql":
        mysql.reset_database()
    elif dbms == "mongo":
        mongo.reset_database()
        mongo.experiment(10000, 50, 3)
    else:
        raise ValueError(f"Invalid DBMS specified: '{dbms}'")


if __name__ == "__main__":
    cli()
