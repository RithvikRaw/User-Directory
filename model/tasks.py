import sys
import os
import poseidons_tools as pt
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
import pandera as pa
import pandas as pd
import pickle
import glob
from datetime import datetime
pd.options.mode.chained_assignment = None

from utils.get_data import connect_to_metabase, get_sql_query, get_user_data_from_metabase

def initialise_pipeline(context: dict):
    context["id"] = datetime.now().strftime("%d%m%Y%H%M%S")
    

def import_training_data(context: dict):
    df = pd.read_csv("model/input/training_data.csv")
    
    context["data"] = df

def import_prediction_data(context: dict):
    df = pd.read_csv("model/input/prediction_data.csv")
    df1 = pd.read_csv("model/input/all_user_data.csv")
    context["prediction_data"] = df
    context["all_users"] = df1

def get_data(query_path: str, pfs: str | list) -> pd.DataFrame:
    user_path = query_path
    connection = connect_to_metabase()
    query = get_sql_query(user_path)
    df = get_user_data_from_metabase(connection, pfs, query)

    return df

def get_training_data(context: dict) -> pd.DataFrame:
    print("Task: Getting Data")

    user_path = "data/sql/training_query.sql"
    df = get_data(user_path, pt.get_platforms())

    context["data"] = df


    return df

def get_prediction_data(context: dict) -> pd.DataFrame:
    print("Task: Getting Data")

    user_path = "data/sql/user_query.sql"
    df1 = get_data(user_path, pt.get_platforms())
    df = df1[df1["days_since_last_login"]<90]
    
    context["prediction_data"] = df
    context["all_users"] = df1

    return df,df1

def process_data(context: dict) -> pd.DataFrame:
    print("Task: Processing Data")
    
    df = context["data"].copy()
    pred_df = context["prediction_data"].copy()
    pred_index = pd.MultiIndex.from_arrays([pred_df["userid"], pred_df["platform"]])
    train_index = pd.MultiIndex.from_arrays([df["userid"], df["platform"]])
    df["y"] = (~train_index.isin(pred_index)).astype(int)
    df["email_MONTHLY"] = (df["email_freq"] == "MONTHLY").astype(int)
    pred_df["email_MONTHLY"] = (pred_df["emailfreq"] == "MONTHLY").astype(int)
    df = df.fillna(0)

    context["processed_data"] = df
    context["prediction_data_processed"] = pred_df


    return df

def validate_schema(context: dict):
    print("Task: Validating Schema")
    df = context["processed_data"].copy()
    schema = pa.DataFrameSchema(
        {
            "total_login_count": pa.Column(int, nullable=False),
            "days_since_last_login": pa.Column(float, nullable=False),
            "email_MONTHLY": pa.Column(int,checks= pa.Check.isin([0, 1]), nullable=False), 
            "y": pa.Column(int, checks= pa.Check.isin([0, 1]), nullable=False)
        }
    )

    schema.validate(df)
    

def train_model(context: dict):
    print("Task: Training Model")
    df = context["processed_data"].copy()
    
    x_columns = ["total_login_count", "days_since_last_login", "email_MONTHLY"]
    df = df[x_columns + ["y"]]
    
    X_train, X_test, y_train, y_test = train_test_split(df[x_columns], df["y"], test_size=0.20, random_state=42)
    
    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    context["model"] = model
    context["test_data"] = (X_test, y_test)
    context["shape"] = df.shape
    
        
def evaluate_model(context: dict):
    print("Task: Evaluating Model")
    model = context["model"]
    X_test, y_test = context["test_data"]
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc_score = roc_auc_score(y_test, y_pred_proba)
    context["auc_score"] = auc_score
    print("AUC-Score: ", auc_score)

def save_model(context: dict):
    print("Task: Saving Model")
    model = context["model"]

    with open(f"model/output/model_{context["id"]}.pkl", "wb") as f:
        pickle.dump(model, f)
    print("Model saved! Congratz")

#load the latest model id.

def load_model(context: dict):
    print("Task: Loading Model")
    model_dir = "model/output"
    model_files = glob.glob(os.path.join(model_dir, "*.pkl"))
    model_files.sort(key=os.path.getmtime, reverse=True)
    latest_model_file = model_files[0]
    with open(latest_model_file, "rb") as f:
        model = pickle.load(f)

    context["model"] = model

def predict(context: dict):
    print("Task: Predicting")
    model = context["model"]
    pf = context["prediction_data_processed"].copy()
    x_columns = ["total_login_count", "days_since_last_login", "email_MONTHLY"]
    df = pf[x_columns]
    probabilities = model.predict_proba(df)[:, 1]
    df.loc[:, "Churn"] = probabilities
    df.drop(columns=x_columns, inplace=True)
    df["userid"] = pf["userid"]
    df["platform"] = pf["platform"]
    context["predicted_data"] = df

def write_to_log(context: dict):
    log = ",".join([context["id"], f"{context['shape'][0]}-{context['shape'][1]}", str(context["auc_score"])])
    with open("model/output/model_log.txt", "a") as f:
        f.write(f"{log}\n")