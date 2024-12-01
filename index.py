from data.processing import df_processed
import streamlit as st

def app():
    st.title("USER DIRECTORY")

    # Extract unique platforms and add 'All' option
    platforms = ['All'] + df_processed['platform'].unique().tolist()

    # Platform filter
    selected_platform = st.selectbox("Select Platform", platforms)

    # Filter DataFrame based on selection
    if selected_platform != 'All':
        df = df_processed[df_processed['Platform'] == selected_platform]

    if st.button("Churned Users"):
        # Display DataFrame or chart for churned users
        st.write("Displaying Churned Users DataFrame")
        # st.dataframe(churned_users_df)
        # st.bar_chart(churned_users_chart)

    if st.button("Churning Users"):
        # Display DataFrame or chart for churning users
        st.write("Displaying Churning Users DataFrame")
        # st.dataframe(churning_users_df)
        # st.line_chart(churning_users_chart)

    if st.button("Stats"):
        # Display statistics charts
        st.write("Displaying Stats")
    