# dashboard.py

# Import required libraries
import streamlit as st
import pandas as pd
import plotly.express as px
import re
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, dateadd, current_timestamp, lit

# Title of the dashboard
st.title("Elastic Common Schema Data Dashboard")

# Create a Snowpark session
def create_session():
    # In Snowflake Streamlit apps, the session is provided automatically
    return Session.builder.getOrCreate()

session = create_session()

def load_data_promql(promql_query, time_window_hours):
    # Ensure time_window_hours is an integer
    time_window_hours = int(time_window_hours)

    # Basic PromQL parsing
    pattern = r'(?P<metric>\w+)(\{(?P<labels>.*)\})?'
    match = re.match(pattern, promql_query.strip())
    if not match:
        st.error("Invalid PromQL query format.")
        return None

    metric_name = match.group('metric')
    labels_str = match.group('labels')

    # Sanitize metric_name
    metric_name = metric_name.replace("'", "''")

    # Build SQL WHERE clause
    where_clauses = []
    where_clauses.append(f'"metricset.name" = \'{metric_name}\'')

    if labels_str:
        # Parse label selectors
        labels = re.findall(r'(\w+)\s*=\s*"(.*?)"', labels_str)
        for key, value in labels:
            # Sanitize key and value
            key = key.replace("'", "''")
            value = value.replace("'", "''")
            where_clauses.append(f'attributes:{key}::STRING = \'{value}\'')

    # Time window filter
    where_clauses.append(f'"@timestamp" >= DATEADD(\'hour\', {time_window_hours}, CURRENT_TIMESTAMP())')

    # Combine WHERE clauses
    where_clause = ' AND '.join(where_clauses)

    # Construct SQL query
    sql_query = f"""
    SELECT "@timestamp", "metric.value", "attributes"
    FROM ecs_schema.metrics
    WHERE {where_clause}
    ORDER BY "@timestamp" DESC
    LIMIT 1000
    """

    # Execute SQL query
    try:
        df = session.sql(sql_query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None


# Function to load data from Snowflake for the dashboard
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

# Sidebar for PromQL query input
st.sidebar.subheader("PromQL Query Input")
promql_query = st.sidebar.text_input("Enter PromQL Query", "", key='promql_query')
promql_time_window_hours = st.sidebar.number_input(
    "Select time window (hours)",
    min_value=1,
    max_value=168,  # Up to one week
    value=24,
    key='promql_time_window'  # Unique key
)
promql_time_window_hours = -abs(promql_time_window_hours)  # Ensure it's negative for dateadd
execute_query = st.sidebar.button("Execute Query", key='execute_promql')

if execute_query and promql_query:
    st.subheader("PromQL Query Results")
    df = load_data_promql(promql_query, promql_time_window_hours)
    if df is not None and not df.empty:
        st.write(df)
        # Visualization
        df['@timestamp'] = pd.to_datetime(df['@timestamp'])
        fig = px.line(df, x='@timestamp', y='metric.value', title='Metric Value Over Time')
        fig.update_xaxes(title='Timestamp')
        fig.update_yaxes(title='Metric Value')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for the given PromQL query.")
else:
    # Existing dashboard functionality
    st.sidebar.subheader("Dashboard Settings")
    # Sidebar for table selection
    table_option = st.sidebar.selectbox(
        "Select ECS Table",
        ("logs", "metrics", "traces"),
        key='table_option'
    )

    # Sidebar for time window selection
    dashboard_time_window_hours = st.sidebar.number_input(
        "Select time window (hours)",
        min_value=1,
        max_value=168,  # Up to one week
        value=24,
        key='dashboard_time_window'  # Unique key
    )
    dashboard_time_window_hours = -abs(dashboard_time_window_hours)  # Ensure it's negative for dateadd

    # Button to refresh data
    if st.sidebar.button('Refresh Data', key='refresh_dashboard'):
        st.cache_data.clear()

    # Load data based on selection
    data_load_state = st.text('Loading data...')
    df = load_data(table_option, dashboard_time_window_hours)
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
            df['@timestamp'] = pd.to_datetime(df['@timestamp'], errors='coerce')
            df = df.dropna(subset=['@timestamp'])

            # Count of logs over time
            fig = px.histogram(df, x='@timestamp', nbins=50, title='Log Entries Over Time')
            fig.update_xaxes(title='Timestamp')
            fig.update_yaxes(title='Number of Logs')
            st.plotly_chart(fig, use_container_width=True)

        elif table_option == "metrics":
            # Parse '@timestamp' to datetime
            df['@timestamp'] = pd.to_datetime(df['@timestamp'], errors='coerce')
            df = df.dropna(subset=['@timestamp'])

            # Select a metric to visualize
            metric_names = df['metricset.name'].unique()
            selected_metric = st.sidebar.selectbox("Select Metric", metric_names, key='selected_metric')

            metric_df = df[df['metricset.name'] == selected_metric]
            if not metric_df.empty:
                fig = px.line(metric_df, x='@timestamp', y='metric.value', title=f"Metric: {selected_metric}")
                fig.update_xaxes(title='Timestamp')
                fig.update_yaxes(title='Metric Value')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No data available for metric '{selected_metric}'.")

        elif table_option == "traces":
            # Parse 'span.start' and 'span.duration'
            df['span.start'] = pd.to_datetime(df['span.start'], errors='coerce')
            df['span.duration'] = pd.to_numeric(df['span.duration'], errors='coerce')
            df = df.dropna(subset=['span.start', 'span.duration'])

            if df.empty:
                st.warning("No valid data available for plotting after cleaning.")
            else:
                # Group data by time interval
                time_group = st.sidebar.selectbox(
                    "Select Time Grouping",
                    options=["Minute", "Hour"],
                    key='time_group'
                )
                if time_group == "Minute":
                    df['time_interval'] = df['span.start'].dt.floor('T')  # Group by minute
                else:
                    df['time_interval'] = df['span.start'].dt.floor('H')  # Group by hour

                duration_df = df.groupby('time_interval')['span.duration'].mean().reset_index()

                if duration_df.empty:
                    st.warning("No data available for plotting after grouping.")
                else:
                    # Proceed to plot
                    fig = px.line(
                        duration_df,
                        x='time_interval',
                        y='span.duration',
                        title='Average Span Duration Over Time'
                    )
                    fig.update_xaxes(title='Timestamp')
                    fig.update_yaxes(title='Average Duration (ms)')
                    st.plotly_chart(fig, use_container_width=True)
