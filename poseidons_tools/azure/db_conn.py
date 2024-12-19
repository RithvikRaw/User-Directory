import os

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker


class Connection:
    """
    Class to connect to a PostgreSQL database and execute queries.

    Args:
        database (str): Name of the database to connect to.

    Attributes:
        config (dict): Configuration parameters for the database connection.
        connection (psycopg2.connection): Connection object to the database. Executed queries are returned as Polars DataFrames.

    Methods:
        connect: Establishes a connection to the database.
        execute: Executes a query on the database and returns the result as a Polars DataFrame.

    Raises:
        Exception: If an error occurs while executing the query. The connection is rolled back and the error is raised.

    Examples:
        >>> conn = Connection("my_database")
        >>> conn.connect()
        >>> df = conn.execute("SELECT * FROM my_table")
        >>> conn.close()
    """

    def __init__(self, database: str):
        load_dotenv()
        self.config = {
            "host": os.environ.get("DB_HOST"),
            "port": os.environ.get("DB_PORT"),
            "database": database,
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PSWD"),
        }
        self.connection = None

    def connect(self):
        engine = create_engine(
            f'postgresql+psycopg2://{self.config["user"]}:{self.config["password"]}@{self.config["host"]}:{self.config["port"]}/{self.config["database"]}'
        )

        self.connection = scoped_session(sessionmaker(bind=engine))

    def execute(self, query):
        try:
            table = self.connection.execute(text(query))
            rows = table.fetchall()
            columns = table.keys()
            df = pl.DataFrame(
                [list(r) for r in rows], schema=list(columns), infer_schema_length=10000
            )
            return df
        except Exception as e:
            self.connection.rollback()
            raise e
