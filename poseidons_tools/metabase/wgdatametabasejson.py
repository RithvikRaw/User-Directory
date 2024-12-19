import json
import os
from functools import singledispatch

import polars as pl
from dotenv import load_dotenv
from metabasepy import Client


@singledispatch
def get_data(
    clients, api_client: Client, query: str, client_dict: dict
) -> pl.DataFrame:
    """
    Function to get data from the database based on the client(s) provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.

    Raises:
        NotImplementedError: If the input type is not supported.
    """
    raise NotImplementedError


@get_data.register
def _(clients: str, api_client: Client, query: str, client_dict: dict) -> dict:
    """
    Function to get data from the database for a single client.

    Args:
        clients (str): Name of the client to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.

    Returns:
        dict: Dictionary containing the data for the client.
    """
    print(f"Getting data for {clients} from Metabase...")
    db = client_dict[clients]["metabase_id"]
    temp_file = "temp.json"
    api_client.dataset.export(
        database_id=db,
        query=query,
        export_format="json",
        full_path=temp_file,
    )
    with open(temp_file, "rb") as file:
        data = json.load(file)
    file.close()
    os.remove(temp_file)
    return data


@get_data.register
def _(clients: list, api_client: Client, query: str, client_dict: dict) -> dict:
    """
    Function to get data from the database for multiple clients.

    Args:
        clients (list): List of clients to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database

    Returns:
        dict: Dictionary containing the data for the clients.
    """
    data = {}
    for client in clients:
        print(f"Getting data for {client} from Metabase...")
        db = client_dict[client]["metabase_id"]
        temp_file = "temp.json"
        api_client.dataset.export(
            database_id=db,
            query=query,
            export_format="json",
            full_path=temp_file,
        )
        with open(temp_file, "rb") as file:
            data[client] = json.load(file)
        file.close()
        os.remove(temp_file)
    return data


class WGDataMetabaseJSON:
    """
    Class to get data from the Metabase database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database
    """

    def __init__(self, clients: str | list[str], sql_query: str) -> None:
        self.clients = clients
        self.sql_query = sql_query

    @property
    def api_client(self) -> Client:
        """
        Property to get the Metabase API client.

        Returns:
            Client: Metabase API client.
        """
        load_dotenv()
        api_client = Client(
            username=os.getenv("METABASE_USER"),
            password=os.getenv("METABASE_PSWD"),
            base_url=os.getenv("METABASE_URL"),
        )
        api_client.authenticate()
        return api_client

    @property
    def client_dict(self):
        path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with open(f"{path}/platforms/list.json", "r") as file:
            client_dict = json.load(file)
        file.close()
        return client_dict

    def get_data(self) -> dict:
        """
        Method to get data from the Metabase database based on the client(s) provided.

        Returns:
            dict: Dictionary containing the data for the client(s).

        Raises:
            NotImplementedError: If the input type is not supported.
        """
        return get_data(self.clients, self.api_client, self.sql_query, self.client_dict)


def get_dict_from_metabase(clients: str | list[str], sql_query: str) -> dict:
    """
    Function to get data from the Metabase database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        sql_query (str, optional): Query to execute on the database

    Returns:
        dict: Dictionary containing the data for the client(s).

    Raises:
        NotImplementedError: If the input type is not supported

    Examples:

    For a single client:
        >>> df = get_data_from_metabase("client1", "SELECT * FROM my_table")

    For multiple clients:
        >>> df = get_data_from_metabase(["client1", "client2"], "SELECT * FROM my_table")

    Using saved queries:
        >>> df = get_data_from_metabase("client1", get_query("my_query"))
    """
    data = WGDataMetabaseJSON(clients, sql_query)
    return data.get_data()
