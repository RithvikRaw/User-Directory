import poseidons_tools as pt
import streamlit as st 
from poseidons_tools import get_platforms

pfs = get_platforms()

@st.cache_resource(show_spinner="Connecting to Metabase...")
def connect_to_metabase() -> pt.MetabaseConnection:
    connection = pt.MetabaseConnection(
        username=st.secrets["username"],
        password=st.secrets["password"]
    )

    return connection

def get_sql_query(query_path: str) -> str:
    with open(query_path) as file:
        sql_query = file.read()
    file.close()

    return sql_query

@st.cache_data(show_spinner="Getting data")
def get_user_data_from_metabase(_connection: pt.MetabaseConnection, platforms: str | list[str], query: str):
    df = _connection.get_data(platforms, query).to_pandas()
    return df