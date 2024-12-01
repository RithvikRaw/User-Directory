import pandas as pd
import polars as pl
import sys
sys.path.append('/Users/rithvikrawat/Downloads/wegrow/Userhub/UserHUB')
from utils.get_data import connect_to_metabase, get_sql_query, get_user_data_from_metabase, pfs

user_path = "/Users/rithvikrawat/Downloads/wegrow/Userhub/UserHUB/data/sql/active_users_info.sql"
connection = connect_to_metabase()
query = get_sql_query(user_path)
df = get_user_data_from_metabase(connection, pfs, query)
columns_to_check = ['userid', 'platform', 'email', 'firstname']

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    df['uid'] = df['userid'].astype(str) + df['platform'].str.lower()
    df_cleaned = df.dropna(subset=columns_to_check)

    df = df_cleaned[(df_cleaned[columns_to_check] != '').all(axis=1)]
    for col in df.columns:
            if col not in columns_to_check:  # Skip the columns_to_check
                if pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    df[col] = df[col].fillna(0)  # Replace nulls with 0 for numeric columns
                else:
                    df[col] = df[col].fillna('unspecified')

    mask = df['firstname'].str.contains(r'\.', na=False)


        # Split 'firstname' at the first dot into two parts
    split_names = df.loc[mask, 'firstname'].str.split('.', n=1, expand=True)

        # Update 'firstname' and 'lastname' columns based on the split
    df.loc[mask, 'firstname'] = split_names[0]
    df.loc[mask, 'lastname'] = split_names[1]

        # Step 2: Merge 'firstname' and 'lastname' into 'name' if 'lastname' is not null or empty
        # Create a mask for rows where 'lastname' is not null or empty
    lastname_mask = df['lastname'].notna() & df['lastname'].ne('')

        # Initialize 'name' with 'firstname'
    df['name'] = df['firstname']

        # Update 'name' by concatenating 'firstname' and 'lastname' where applicable
    df.loc[lastname_mask, 'name'] = df.loc[lastname_mask, 'firstname'] + ' ' + df.loc[lastname_mask, 'lastname']
    
    return df

df_processed = process_data(df)

print(df_processed)