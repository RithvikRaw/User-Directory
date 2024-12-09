import pandas as pd
from model.pipeline import prediction_pipeline

pred_df, df = prediction_pipeline()

df_pred = df[df["days_since_last_login"] < 90]
df_pred = df_pred.merge(pred_df, how="left", on=["userid", "platform"])

df_pred['Churn%'] = df_pred['Churn'].apply(lambda x: round(x * 100, 2) if pd.notna(x) else None)

df = df.merge(df_pred[['userid', 'platform', 'Churn%']], on=['userid', 'platform'], how='left')

order = [
    'name', 'userid', 'platform', 'Churn%', 'days_since_last_login', 'age_on_platform', 
    'total_login_count', 'country', 'department', 'emailfreq', 'level', 
    'last_login', 'firstname', 'lastname', 'email'
]

for column in order:
    if column not in df.columns:
        df[column] = None
    if column not in df_pred.columns:
        df_pred[column] = None

df = df[order]
df_pred = df_pred[order]

df = df[~df['email'].str.contains("@wegrow-app.com", na=False)]
df_pred = df_pred[~df_pred['email'].str.contains("@wegrow-app.com", na=False)]