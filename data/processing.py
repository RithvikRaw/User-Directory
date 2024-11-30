import pandas as pd

from utils.get_data import connect_to_metabase, get_sql_query, get_user_data_from_metabase

user_path = "data/sql/users.sql"
connection = connect_to_metabase()
query = get_sql_query(user_path)
df = get_user_data_from_metabase(connection, "Henkel ACM", query)

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ones"] = pd.Series([1] * len(df))
    return df

df_processed = process_data(df)