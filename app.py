import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import time
import plotly.express as px

# Set page configuration for a wider layout
st.set_page_config(page_title="RealTime Dashboard", layout="wide")

# PostgreSQL connection details
DB_HOST = "192.168.56.1"
DB_PORT = 5433  # Updated port
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Streamlit app
st.title("RealTime Dashboard Using Streamlit")

# Create two columns for layout control
col1, col2 = st.columns([1, 5])  # Narrower width for dropdown

with col1:
    # Dropdown filter for time range in the smaller column
    time_range = st.selectbox(
        "Select Time Range",
        options=["1 minute", "5 minutes", "10 minutes", "30 minutes","1 hour"],
        index=1
    )

# Convert selected time range to minutes
time_mapping = {"1 minute": 1, "5 minutes": 5, "10 minutes": 10, "30 minutes": 30, "1 hour": 60}
selected_minutes = time_mapping[time_range]

# Function to connect to PostgreSQL and fetch data
def fetch_data(selected_minutes):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        # Current time and time range based on the selection
        now = datetime.now()
        start_time = now - timedelta(minutes=selected_minutes)

        # Convert to string format for SQL compatibility
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

        # Query to get calls in the selected time range
        kpi_query = """
        SELECT sum(calls) AS calls_in_range
        FROM pyd.real_time
        WHERE date >= %s
        """
        
        # Fetch calls in the selected range
        kpi_df = pd.read_sql_query(kpi_query, conn, params=[start_time_str])
        calls_in_range = kpi_df['calls_in_range'][0]
        
        # Handle cases where no data is returned
        if pd.isna(calls_in_range):
            calls_in_range = 0

        # Query to fetch data for the line chart
        line_chart_query = """
        SELECT date, calls
        FROM pyd.real_time
        WHERE date >= %s
        """
        
        # Fetch data for the line chart
        df = pd.read_sql_query(line_chart_query, conn, params=[start_time_str])
        
        # Close the connection
        conn.close()

        # Format the 'date' column to show time in minute:second format
        df['time_formatted'] = pd.to_datetime(df['date']).dt.strftime('%M:%S')
        
        return int(calls_in_range), df

    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return 0, None  # Return default values if an error occurs

# Fetch data based on the selected time range
calls_in_range, df = fetch_data(selected_minutes)

# Display the KPI on top
if calls_in_range is not None:
    st.metric(f"Calls in Last {selected_minutes} Minutes", value=calls_in_range)

# Display the line chart below
if df is not None and not df.empty:
    # Plotly chart for more control
    fig = px.line(
        df,
        x='time_formatted',
        y='calls',
        title="Calls Over Time",
        labels={'time_formatted': 'Time (MM:SS)', 'calls': 'Number of Calls'}
    )
    fig.update_layout(
        width=1200,  # Set custom width
        height=500,  # Set custom height
        title_x=0.5,  # Center the title
        xaxis=dict(tickformat="%M:%S"),  # Ensure time format shows minutes and seconds only
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.write("No data available for the selected time range.")

# Real-time updates by rerunning the script every minute
time.sleep(5)  # Adjust sleep time if needed

# Trigger a rerun of the Streamlit app
st.rerun()
