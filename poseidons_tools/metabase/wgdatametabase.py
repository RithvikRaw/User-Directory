import json
import os
from functools import singledispatch

import polars as pl
from dotenv import load_dotenv
from metabasepy import Client


@singledispatch
def get_data(
    clients,
    api_client: Client,
    query: str,
    client_dict: dict,
    download_format: str = "csv",
) -> pl.DataFrame:
    """
    Function to get data from the database based on the client(s) provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.
        download_format (str): Format to download the data in.

    Raises:
        NotImplementedError: If the input type is not supported.
    """
    raise NotImplementedError


@get_data.register
def _(
    clients: str,
    api_client: Client,
    query: str,
    client_dict: dict,
    download_format: str = "csv",
) -> pl.DataFrame:
    """
    Function to get data from the database for a single client.

    Args:
        clients (str): Name of the client to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.
        download_format (str): Format to download the data in.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the client.
    """
    print(f"Getting data for {clients} from Metabase...")
    db = client_dict[clients]["metabase_id"]
    temp_file = f"temp.{download_format}"
    api_client.dataset.export(
        database_id=db,
        query=query,
        export_format=download_format,
        full_path=temp_file,
    )
    match download_format:
        case "csv":
            df = pl.read_csv(temp_file)
        case "json":
            df = pl.read_json(temp_file)
        case "xlsx":
            df = pl.read_excel(temp_file)
    os.remove(temp_file)
    return df


@get_data.register
def _(
    clients: list,
    api_client: Client,
    query: str,
    client_dict: dict,
    download_format: str = "csv",
) -> pl.DataFrame:
    """
    Function to get data from the database for multiple clients.

    Args:
        clients (list): List of clients to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.
        download_format (str): Format to download the data in.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the clients.
    """
    df = pl.DataFrame()
    for client in clients:
        print(f"Getting data for {client} from Metabase...")
        db = client_dict[client]["metabase_id"]
        temp_file = f"temp.{download_format}"
        api_client.dataset.export(
            database_id=db,
            query=query,
            export_format=download_format,
            full_path=temp_file,
        )
        match download_format:
            case "csv":
                df_temp = pl.read_csv(temp_file)
            case "json":
                df_temp = pl.read_json(temp_file)
            case "xlsx":
                df_temp = pl.read_excel(temp_file)

        if df_temp.is_empty():
            continue
        df_temp = df_temp.with_columns(pl.lit(client).alias("Client"))
        df = pl.concat([df, df_temp])
        os.remove(temp_file)
    return df


class WGDataMetabase:
    """
    Class to get data from the Metabase database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        api_client (Client): Metabase API client.
        query (str): Query to execute on the database.
        client_dict (dict): Dictionary containing the client names and their corresponding database.
        download_format (str): Format to download the data in.
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

    def get_data(self, download_format: str = "csv") -> pl.DataFrame:
        """
        Method to get data from the Metabase database based on the client(s) provided.

        Args:
            download_format (str): Format to download the data in.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data for the client(s).

        Raises:
            NotImplementedError: If the input type is not supported.
            ValueError: If the download format is not 'csv' or 'json'.
        """
        if download_format not in ["csv", "json", "xlsx"]:
            raise ValueError("Download format must be 'csv', 'json', or 'xlsx'.")
        return get_data(
            self.clients,
            self.api_client,
            self.sql_query,
            self.client_dict,
            download_format,
        )


def get_data_from_metabase(
    clients: str | list[str], sql_query: str, download_format: str = "csv"
) -> pl.DataFrame:
    """
    Function to get data from the Metabase database for the clients provided.

    Args:
        clients (str | list[str]): Name of the client(s) to get data for.
        sql_query (str, optional): Query to execute on the database.
        download_format (str, optional): Format to download the data in. Defaults to 'csv'.

    Returns:
        pl.DataFrame: Polars DataFrame containing the data for the client(s).

    Raises:
        NotImplementedError: If the input type is not supported.
        ValueError: If the download format is not 'csv', 'json' or 'xlsx'.

    Examples:

    For a single client:
        >>> df = get_data_from_metabase("client1", "SELECT * FROM my_table")

    For multiple clients:
        >>> df = get_data_from_metabase(["client1", "client2"], "SELECT * FROM my_table")

    Using saved queries:
        >>> df = get_data_from_metabase("client1", get_query("my_query"))

    Using a different download format:
        >>> df = get_data_from_metabase("client1", "SELECT * FROM my_table", "json")
    """
    data = WGDataMetabase(clients, sql_query)
    return data.get_data(download_format)
