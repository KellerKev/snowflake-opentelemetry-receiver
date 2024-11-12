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

def transpile_spl_to_sql(spl_query):
    select_clause = "SELECT *"
    where_clause = ""
    group_by_clause = ""
    table_name = "ecs_schema.metrics"

    spl_query = spl_query.strip()
    search_match = re.match(r'search\s+(.*)', spl_query)
    if search_match:
        conditions = search_match.group(1)
        where_conditions = []
        
        for condition in re.findall(r'(\w+)\s*=\s*"([^"]+)"', conditions):
            field, value = condition
            if field == "metric_name":
                where_conditions.append(f'"metricset.name" = \'{value}\'')
            else:
                # Correct access to attributes field with double quotes
                where_conditions.append(f'"attributes"["{field}"]::STRING = \'{value}\'')
        
        where_clause = "WHERE " + " AND ".join(where_conditions)

    table_match = re.search(r'\|\s*table\s+(.*)', spl_query)
    if table_match:
        fields = table_match.group(1).split(", ")
        select_clause = "SELECT " + ", ".join([
            f'"attributes"["{field}"]::STRING AS {field}' if field not in ["@timestamp", "metric.value"] else f'"{field}"'
            for field in fields
        ])

    stats_match = re.search(r'\|\s*stats\s+(.*)', spl_query)
    if stats_match:
        stats_clause = stats_match.group(1)
        if "avg(" in stats_clause:
            match = re.search(r'avg\(([^)]+)\)\s+by\s+(\w+)', stats_clause)
            if match:
                metric, group_by_field = match.groups()
                select_clause = f'''SELECT \"attributes\"[\"{group_by_field}\"]::STRING AS {group_by_field}, AVG(\"{metric}\") AS avg_value"
                group_by_clause = f'GROUP BY {group_by_field}'''

    sql_query =f'''
    {select_clause}
    FROM {table_name}
    {where_clause}
    {group_by_clause}
    ORDER BY "@timestamp" DESC
    LIMIT 1000'''
    
    return sql_query.strip()







# Function to execute the SPL query
def execute_spl_query(spl_query):
    sql_query = transpile_spl_to_sql(spl_query)
    try:
        df = session.sql(sql_query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error executing SPL query: {e}")
        return None

# SPL Query Form
st.sidebar.subheader("SPL Query Input")
spl_query = st.sidebar.text_input("Enter SPL Query", "")
execute_spl_query_button = st.sidebar.button("Execute SPL Query")

if execute_spl_query_button and spl_query:
    st.subheader("SPL Query Results")
    df = execute_spl_query(spl_query)
    if df is not None and not df.empty:
        st.write(df)
        # Optional: Add visualization if relevant fields are present
        if "@timestamp" in df.columns and "metric.value" in df.columns:
            df['@timestamp'] = pd.to_datetime(df['@timestamp'])
            fig = px.line(df, x='@timestamp', y='metric.value', title='Metric Value Over Time')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for the given SPL query.")
