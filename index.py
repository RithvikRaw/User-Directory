from data.processing import df, df_pred
import streamlit as st

def app():
    global df, df_pred

    st.markdown("""
                    # UserHUB 👤
                    ## Access User Info ⚡️
                    ---
                    """)

    search_input = st.sidebar.text_input('Search User by ID, Name, Email', '')
    status = st.sidebar.selectbox("User Status", ['All', 'Active', 'Inactive'])

    if status == 'All':
        df_processed = df
    elif status == 'Active':
        df_processed = df_pred
        churn_slider = st.sidebar.slider('Filter by Churn Percentage', 0, 100, (0, 100))
        df_processed = df_processed[(df_processed['Churn%'] >= churn_slider[0]) & (df_processed['Churn%'] <= churn_slider[1])]
    else:
        df_processed = df[~df.set_index(['userid', 'platform']).index.isin(df_pred.set_index(['userid', 'platform']).index)]

    # Select filters for platform, region, department, level
    platform_counts = df_processed['platform'].value_counts().sort_values(ascending=False)
    region_counts = df_processed['country'].value_counts().sort_values(ascending=False)
    department_counts = df_processed['department'].value_counts().sort_values(ascending=False)
    level_counts = df_processed['level'].value_counts().sort_values(ascending=False)

    selected_platform = st.sidebar.selectbox("Platform", ['All'] + list(platform_counts.index))
    selected_region = st.sidebar.selectbox("Region", ['All'] + list(region_counts.index))
    selected_department = st.sidebar.selectbox("Department", ['All'] + list(department_counts.index))
    selected_level = st.sidebar.selectbox("Level", ['All'] + list(level_counts.index))

    # Apply filters
    if selected_platform != 'All':
        df_processed = df_processed[df_processed['platform'] == selected_platform]
    if selected_region != 'All':
        df_processed = df_processed[df_processed['country'] == selected_region]
    if selected_department != 'All':
        df_processed = df_processed[df_processed['department'] == selected_department]
    if selected_level != 'All':
        df_processed = df_processed[df_processed['level'] == selected_level]

    # Update and display KPIs after filtering
    total_users = len(df_processed)
    active_users = len(df_processed[df_processed['Churn%'] <= 25])  # Active user definition might need adjustment
    high_risk_users = df_processed[df_processed['Churn%'] > 75].shape[0]

    st.sidebar.markdown('---')
    st.sidebar.markdown("""
                ## Key Performance Indicators 📊
                """)
    st.sidebar.markdown(f"Total WeGrow Users 📚: `{total_users}`")
    if total_users > 0:
        active_ratio = active_users / total_users * 100
        st.sidebar.markdown(f"Total Active WeGrow Users 🌟: `{active_users}` ({active_ratio:.2f}%)")
    st.sidebar.markdown(f"High Risk Users ⚠️: `{high_risk_users}`")
    
    if search_input:
        df_processed = df_processed[df_processed.apply(lambda row: row[['firstname', 'lastname', 'name', 'email']].astype(str).str.contains(search_input, case=False, na=False).any(), axis=1)]
   
    st.dataframe(df_processed, height=600)
        
            
