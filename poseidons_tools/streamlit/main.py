import json
import os

import polars as pl
from metabasepy import Client

from poseidons_tools.metabase.wgdatametabase import get_data


class MetabaseConnection:
    """
    Create a connection to the Metabase database to get data for the client(s).

    Args:
        username (str): Metabase username.
        password (str): Metabase password.
        base_url (str, optional): Metabase base URL.

    Attributes:
        api_client (Client): Authenticated Metabase API client.
    """

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str = "https://metabase.wegrow-lab.com/",
    ):
        self.username = username
        self.password = password
        self.base_url = base_url

        self.api_client = Client(
            username=username,
            password=password,
            base_url=base_url,
        )
        self.api_client.authenticate()

    @property
    def client_dict(self):
        path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with open(f"{path}/platforms/list.json", "r") as file:
            client_dict = json.load(file)
        file.close()
        return client_dict

    def get_data(
        self, clients: str | list[str], sql_query: str, download_format: str = "csv"
    ) -> pl.DataFrame:
        """
        Method to get data from the Metabase database for the clients provided.

        Args:
            clients (str | list[str]): Name of the client(s) to get data for.
            sql_query (str): Query to execute on the database.
            download_format (str, optional): Format to download the data in. Defaults to 'csv'.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data for the client(s).

        Raises:
            NotImplementedError: If the input type is not supported.
            ValueError: If the download format is not 'csv', 'json' or 'xlsx'.
            ValueError: If the client name is not found in the list of clients.
            EnvironmentError: If the Metabase username, password, or URL is not provided in the .env file.

        Examples:

        For a single client:
            >>> connection = MetabaseConnection("username", "password")
            >>> df = connection.get_data("client1", "SELECT * FROM my_table")

        For multiple clients:
            >>> df = connection.get_data(["client1", "client2"], "SELECT * FROM my_table")

        Using saved queries:
            >>> df = connection.get_data("client1", get_query("my_query"))

        Using a different download format:
            >>> df = connection.get_data("client1", "SELECT * FROM my_table", "json")
        """
        client_dict = self.client_dict
        if isinstance(clients, str) and clients not in client_dict.keys():
            raise ValueError(
                f"Client {clients} not found in the list of clients. Please check the spelling or use get_platforms() to get the list of available clients."
            )
        if isinstance(clients, list):
            for client in clients:
                if client not in client_dict.keys():
                    raise ValueError(
                        f"Client {client} not found in the list of clients. Please check the spelling or use get_platforms() to get the list of available clients."
                    )
        if download_format not in ["csv", "json", "xlsx"]:
            raise ValueError("Download format must be 'csv', 'json', or 'xlsx'.")
        return get_data(
            clients,
            self.api_client,
            sql_query,
            client_dict,
            download_format,
        )
