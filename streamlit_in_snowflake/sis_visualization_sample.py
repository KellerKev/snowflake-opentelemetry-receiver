# dashboard.py

# Import required libraries
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, dateadd, current_timestamp, lit

# Title of the dashboard
st.title("Elastic Common Schema Data Dashboard")

# Create a Snowpark session
def create_session():
    # In Snowflake Streamlit apps, the session is provided automatically
    return Session.builder.getOrCreate()

session = create_session()

# Function to load data from Snowflake
@st.cache_data(ttl=10)
def load_data(table_name, time_window_hours):
    # Import necessary functions
    from snowflake.snowpark.functions import col, dateadd, current_timestamp, lit

    # Calculate the timestamp from the desired time window
    time_threshold = dateadd('hour', lit(time_window_hours), current_timestamp())

    # Determine the timestamp column based on the table
    if table_name == 'traces':
        timestamp_col = '"span.start"'
    else:
        timestamp_col = '"@timestamp"'

    # Query data using Snowpark
    df = session.table(f"ecs_schema.{table_name}") \
        .filter(col(timestamp_col) >= time_threshold) \
        .sort(col(timestamp_col).desc()) \
        .limit(1000) \
        .to_pandas()
    return df

# Sidebar for table selection
table_option = st.sidebar.selectbox(
    "Select ECS Table",
    ("logs", "metrics", "traces")
)

# Sidebar for time window selection
time_window_hours = st.sidebar.number_input(
    "Select time window (hours)",
    min_value=1,
    max_value=168,  # Up to one week
    value=24
)
time_window_hours = -abs(time_window_hours)  # Ensure it's negative for dateadd

# Button to refresh data
if st.sidebar.button('Refresh Data'):
    st.cache_data.clear()

# Load data based on selection
data_load_state = st.text('Loading data...')
df = load_data(table_option, time_window_hours)
data_load_state.text('Loading data...done!')

# Check if DataFrame is empty
if df.empty:
    st.warning(f"No data available for the selected time window in {table_option} table.")
else:
    # Display data
    st.subheader(f"Latest data from {table_option} table")
    st.write(df)

    # Visualization
    st.subheader("Visualization")

    if table_option == "logs":
        # Parse '@timestamp' to datetime
        df['@timestamp'] = pd.to_datetime(df['@timestamp'])

        # Count of logs over time
        fig = px.histogram(df, x='@timestamp', nbins=50, title='Log Entries Over Time')
        fig.update_xaxes(title='Timestamp')
        fig.update_yaxes(title='Number of Logs')
        st.plotly_chart(fig, use_container_width=True)

    elif table_option == "metrics":
        # Parse '@timestamp' to datetime
        df['@timestamp'] = pd.to_datetime(df['@timestamp'])

        # Select a metric to visualize
        metric_names = df['metricset.name'].unique()
        selected_metric = st.sidebar.selectbox("Select Metric", metric_names)

        metric_df = df[df['metricset.name'] == selected_metric]
        fig = px.line(metric_df, x='@timestamp', y='metric.value', title=f"Metric: {selected_metric}")
        fig.update_xaxes(title='Timestamp')
        fig.update_yaxes(title='Metric Value')
        st.plotly_chart(fig, use_container_width=True)

    elif table_option == "traces":
        # Parse 'span.start' and 'span.duration'
        df['span.start'] = pd.to_datetime(df['span.start'], errors='coerce')
        df['span.duration'] = pd.to_numeric(df['span.duration'], errors='coerce')
        
        # Display data types and check for null values
        #st.write("Data Types:")
        #st.write(df.dtypes)
        st.write("Null values in 'span.start':", df['span.start'].isnull().sum())
        st.write("Null values in 'span.duration':", df['span.duration'].isnull().sum())
        
        # Display first few rows
        #st.write("DataFrame df after conversion:")
        #st.write(df.head())
        
        # Drop rows with NaN values
        df = df.dropna(subset=['span.start', 'span.duration'])
        
        # Check if df is empty after dropping NaNs
        if df.empty:
            st.warning("No valid data available for plotting after cleaning.")
        else:
            # Group data by minute
            df['minute'] = df['span.start'].dt.floor('T')
            duration_df = df.groupby('minute')['span.duration'].mean().reset_index()
            
            # Display duration_df
            st.write("DataFrame duration_df:")
            st.write(duration_df.head())
            
            if duration_df.empty:
                st.warning("No data available for plotting after grouping.")
            else:
                # Proceed to plot
                fig = px.line(duration_df, x='minute', y='span.duration', title='Average Span Duration Over Time')
                fig.update_xaxes(title='Timestamp')
                fig.update_yaxes(title='Average Duration (ms)')
                st.plotly_chart(fig, use_container_width=True)

