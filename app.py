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

import bcrypt

hashed_password = b'$2b$12$w9SLU1Nud2.us8w8JtOBqOOCfHz3fOXmSHkuiUNShOYoCntHqtXQi'

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

def check_password(hashed_password, user_password):
    if bcrypt.checkpw(user_password.encode('utf-8'), hashed_password):
        st.session_state['authenticated'] = True
    else:
        st.session_state['authenticated'] = False
        st.error("Incorrect password, try again!")

if not st.session_state['authenticated']:
    password_input = st.text_input("Password", type="password", key='password')
    if password_input:
        check_password(hashed_password, password_input)

if st.session_state['authenticated']:
    app()