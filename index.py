from data.processing import df_processed
import streamlit as st

def app():
    st.title("UserHUB : user directory")
    st.dataframe(df_processed)