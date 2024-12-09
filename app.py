import streamlit as st

st.set_page_config(
    page_title='User Directory',  
    page_icon='📖',             
    layout='wide',   
    initial_sidebar_state='expanded', 
    menu_items={
        'About': "User Directory. Note: Churn is a 90 day inactivity period"
    }
)

from index import app

app()