import json
import os
from functools import singledispatch

import polars as pl

from poseidons_tools.azure.db_conn import Connection


@singledispatch
def get_data(clients: str | list, query: str, client_dict: dict) -> pl.DataFrame:
    """
    Function to get data from the database based on the client(s) provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.

    Raises:
        NotImplementedError: If the input type is not supported.
    """
    raise NotImplementedError


@get_data.register
def _(clients: str, query: str, client_dict: dict) -> pl.DataFrame:
    """
    Function to get data from the database for a single client.

    Args:
        clients (str): Name of the client to get data for.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the client.
    """
    db = client_dict[clients]["azure_db"]
    print(f"Getting data for {clients} from {db}...")
    connection = Connection(db)
    connection.connect()
    df = connection.execute(query)
    return df


@get_data.register
def _(clients: list, query: str, client_dict: dict) -> pl.DataFrame:
    """
    Function to get data from the database for multiple clients.

    Args:
        clients (list): List of clients to get data for.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the clients.
    """
    df = pl.DataFrame()
    for client in clients:
        db = client_dict[client]["azure_db"]
        print(f"Getting data for {client} from {db}...")
        connection = Connection(db)
        connection.connect()
        df_temp = connection.execute(query)
        if df_temp.is_empty():
            continue
        else:
            df_temp = df_temp.with_columns(pl.lit(client).alias("Client"))
            df = pl.concat([df, df_temp])
    return df


class WGDataAzure:
    """
    Class to get data from the Azure database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        sql_query (str): Query to execute on the database.

    Attributes:
        clients (str | list[str]): Name of the client(s) to get data for.
        sql_query (str): Query to execute on the database.

    Methods:
        get_data: Gets the data from the database based on the client(s) provided.

    Raises:
        NotImplementedError: If the input type is not supported.

    Examples:

    For a single client:
        >>> data = WGDataAzure("client1", "SELECT * FROM my_table")
        >>> df = data.get_data()

    For multiple clients:
        >>> data = WGDataAzure(["client1", "client2"], "SELECT * FROM my_table")
        >>> df = data.get_data()

    Using saved queries:
        >>> data = WGDataAzure("client1", get_query("my_query"))
        >>> df = data.get_data()
    """

    def __init__(self, clients: str | list[str], sql_query: str):
        self.clients = clients
        self.sql_query = sql_query

    @property
    def client_dict(self):
        path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with open(f"{path}/platforms/list.json", "r") as file:
            client_dict = json.load(file)
        file.close()
        return client_dict

    def get_data(self) -> pl.DataFrame:
        return get_data(self.clients, self.sql_query, self.client_dict)


def get_data_from_azure(clients: str | list[str], sql_query: str) -> pl.DataFrame:
    """
    Function to get data from the Azure database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        sql_query (str): Query to execute on the database.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the client(s).

    Raises:
        NotImplementedError: If the input type is not supported.

    Examples:

    For a single client:
        >>> df = get_data_from_azure("client1", "SELECT * FROM my_table")

    For multiple clients:
        >>> df = get_data_from_azure(["client1", "client2"], "SELECT * FROM my_table")

    Using saved queries:
        >>> df = get_data_from_azure("client1", get_query("my_query"))
    """
    data = WGDataAzure(clients, sql_query)
    return data.get_data()
