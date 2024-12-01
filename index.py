from data.processing import df_processed
import streamlit as st

def app():
    l1, c1, r1 = st.columns([1, 2, 1])
    with c1:
        st.title('User Directory 📖')

    # Filters as a top bar in the main area
    col1, col2, col3, col4 = st.columns(4)
    platform_counts = df_processed['platform'].value_counts().sort_values(ascending=False)
    platforms = ['All'] + platform_counts.index.tolist()
    selected_platform = col1.selectbox("Platform", platforms)

    region_counts = df_processed['country'].value_counts().sort_values(ascending=False)
    regions = ['All'] + region_counts.index.tolist()
    selected_region = col2.selectbox("Region", regions)

    department_counts = df_processed['department'].value_counts().sort_values(ascending=False)
    departments = ['All'] + department_counts.index.tolist()
    selected_department = col3.selectbox("Department", departments)

    levels = ['All'] + df_processed['level'].unique().tolist()
    selected_level = col4.selectbox("Level", levels)

    s1, s2 =st.columns([1,5])
    with s1:
        st.write("Search for a User 🔍")
    with s2:
        user_query = st.text_input("Enter User-id, Email, or Name of a User:", "")

    # Applying filters
    df = df_processed.copy()
    if selected_platform != 'All':
        df = df[df['platform'] == selected_platform]
    if selected_region != 'All':
        df = df[df['country'] == selected_region]
    if selected_department != 'All':
        df = df[df['department'] == selected_department]
    if selected_level != 'All':
        df = df[df['level'] == selected_level]

    # Apply search filter based on input type
    if user_query:
        if user_query.isdigit():  # Assuming user_id is numeric
            user_query_number = int(user_query)
            # Ensure 'userid' in DataFrame is also treated as integers, then perform the comparison
            df = df[df['userid'].astype(int) == user_query_number]
        elif '@' in user_query:
            df = df[df['email'].str.contains(user_query, case=False, na=False)]
        else:
            df = df[df['name'].str.contains(user_query, case=False, na=False)]

    # Show search results in an expander
    if not df.empty:
        with st.expander("Search Results"):
            df_display = df[['userid', 'platform', 'name', 'email']].reset_index(drop=True)
            st.dataframe(df_display, use_container_width=True)
    
    l2, c2, r2 = st.columns([1, 1, 1])

    # Popover for Most Engaged Users
    with l2:
        most_engaged = st.expander("Most Engaged Users")
        most_engaged.write("Details will be added here...")  # Placeholder text

    # Popover for Least Engaged Users
    with c2:
        least_engaged = st.expander("Least Engaged Users")
        least_engaged.write("Details will be added here...")  # Placeholder text

    # Popover for Inactive Users
    with r2:
        inactive_users = st.expander("Inactive Users")
        inactive_users.write("Details will be added here...") 
        
            
