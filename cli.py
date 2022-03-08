import pg
import mysql
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
        db = pg.Connector()
        with open("pg_schema.sql", "r") as file:
            db.cursor.execute(file.read())
    elif dbms == "mysql":
        db = mysql.Connector()
        with open("mysql_schema.sql", "r") as file:
            db.cursor.execute(file.read())
    db.close()


if __name__ == "__main__":
    cli()
