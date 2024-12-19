from data.processing import df, df_pred
import streamlit as st
import pandas as pd

csm = pd.read_csv('csm.csv')

def app():
    
    data = df.copy()
    data['last_login'] = pd.to_datetime(data['last_login']).dt.date
    active_data = df_pred.copy()
    active_data['last_login'] = pd.to_datetime(active_data['last_login']).dt.date
    
    colA, colB, colC = st.columns(3)
    colA.markdown("""
                    # ⨭UserHEALTH⨮
                    """)
    
    st.markdown("---")
    
    if 'show_advanced' not in st.session_state:
        st.session_state.show_advanced = False
    
    if 'csm' not in st.session_state:
        st.session_state.csm = False   

    col1, col2 = st.columns([1,2])
    with col1:
        col5, col6 = st.columns(2)
        with col5:
            if st.button("CSM"):
                st.session_state.csm = not st.session_state.csm
        with col6:
            if st.button("Advanced Settings"):
                st.session_state.show_advanced = not st.session_state.show_advanced 
                
    with col2:
        col3, col7, col8 = st.columns([4,1,1])
        with col3:
            search_input = st.text_input('Search User by ID, Name, Email 🔎', '')
        with col8:
            active_toggle = st.toggle("Active Users", value=True)

    if active_toggle:
        filtered_df = active_data
        filtered_df = filtered_df.sort_values(by='Churn%', ascending=False)
    else:       
        filtered_df = data
        
    if st.session_state.csm:
        csm_count = csm['Unnamed: 6'].unique()
        with col5:
            selected_csm = st.multiselect("", ["All"] + list(csm_count), default=['All'])

            if 'All' in selected_csm:
                csm_platforms = csm['Unnamed: 1'].unique()
            else:
                csm_platforms = csm[csm['Unnamed: 6'].isin(selected_csm)]['Unnamed: 1'].unique()

            if 'All' not in selected_csm:
                filtered_df = filtered_df[filtered_df['platform'].isin(csm_platforms)]  
        
    if st.session_state.show_advanced:
        platform_counts = filtered_df['platform'].value_counts().sort_values(ascending=False)
        region_counts = filtered_df['country'].value_counts().sort_values(ascending=False)
        department_counts = filtered_df['department'].value_counts().sort_values(ascending=False)
        level_counts = filtered_df['level'].value_counts().sort_values(ascending=False)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            selected_platforms = st.multiselect("Platform", ['All'] + list(platform_counts.index), default=['All'])
        with col2:
            selected_regions = st.multiselect("Region", ['All'] + list(region_counts.index), default=['All'])
        with col3:
            selected_departments = st.multiselect("Department", ['All'] + list(department_counts.index), default=['All'])
        with col4:
            selected_levels = st.multiselect("Level", ['All'] + list(level_counts.index), default=['All'])

        if 'All' not in selected_platforms:
            filtered_df = filtered_df[filtered_df['platform'].isin(selected_platforms)]
        if 'All' not in selected_regions:
            filtered_df = filtered_df[filtered_df['country'].isin(selected_regions)]
        if 'All' not in selected_departments:
            filtered_df = filtered_df[filtered_df['department'].isin(selected_departments)]
        if 'All' not in selected_levels:
            filtered_df = filtered_df[filtered_df['level'].isin(selected_levels)]
            
    active_users = len(filtered_df[filtered_df["Churn%"].notnull()])
    high_risk_users = len(filtered_df[filtered_df['Churn%'] > 75])
    if active_users == 0:
        high_risk_ratio = 0
    else:
        high_risk_ratio = high_risk_users / active_users * 100

    colB.subheader("📈 Active Users")
    colB.info(f"**{active_users}** users interacted in the last 90 days")
    colC.subheader("⚠️ High Risk Users")
    colC.warning(f"**{high_risk_users}** ({high_risk_ratio:.2f}%) users have more than 75% churn probability")
    
    if search_input:
        if search_input.isdigit():
            search_filtered_df = filtered_df[filtered_df['userid'] == int(search_input)]
        else:
            search_filtered_df = filtered_df[
                filtered_df['name'].str.contains(search_input, case=False, na=False) |
                filtered_df['email'].str.contains(search_input, case=False, na=False)
            ]
        
        if not search_filtered_df.empty:
            with st.expander(f"**{len(search_filtered_df)}**"+" results found", expanded=True):
                st.dataframe(search_filtered_df.set_index(['userid', 'platform']), use_container_width=True)
        else:
            st.info("No matches found.")
    else:
        with st.expander(f"**{len(filtered_df)}**"+" users displayed", expanded=True):
                st.dataframe(filtered_df.set_index(['userid', 'platform']), use_container_width=True)
            
        st.markdown("---")