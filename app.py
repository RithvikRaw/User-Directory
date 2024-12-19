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

import hmac

def check_password():  
    def password_entered():  
        if hmac.compare_digest(st.session_state["password"], st.secrets["app_password"]):  
            st.session_state["password_correct"] = True  
            del st.session_state["password"]
        else:  
            st.session_state["password_correct"] = False  
    if st.session_state.get("password_correct", False):  
        return True  
    st.text_input(  
        "Password", type="password", on_change=password_entered, key="password"  
    )  
    if "password_correct" in st.session_state:  
        st.error("😕 Password incorrect")  
    return False

if not check_password():  
    st.stop()
    
app()