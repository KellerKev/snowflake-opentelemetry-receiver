-- Create Streams on the source tables
CREATE OR REPLACE STREAM logs_stream ON TABLE logs APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM metrics_stream ON TABLE metrics APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM traces_stream ON TABLE traces APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE ecs_transform_incremental()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session):
    from snowflake.snowpark.functions import col, parse_json, datediff, lit

    try:
        # Create the target schema if it doesn't exist
        session.sql("CREATE SCHEMA IF NOT EXISTS ecs_schema").collect()

        # ***********************
        # Process Logs
        # ***********************

        # Create the target 'logs' table if it doesn't exist
        session.sql("""
            CREATE TABLE IF NOT EXISTS ecs_schema.logs (
                "@timestamp" TIMESTAMP_NTZ,
                "message" STRING,
                "log.level" STRING,
                "attributes" VARIANT
            )
        """).collect()

        # Read new data from the logs_stream
        logs_df = session.table("logs_stream")

        # Check if there is new data to process
        if logs_df.count() > 0:
            # Transform and insert data into ecs_schema.logs
            logs_transformed = logs_df.select(
                col("timestamp").alias("@timestamp"),
                col("message"),
                col("log_level").alias("log.level"),
                parse_json(col("attributes")).alias("attributes")
            )

            logs_transformed.write.mode("append").save_as_table("ecs_schema.logs")
        else:
            print("No new logs data to process.")

        # ***********************
        # Process Metrics
        # ***********************

        # Create the target 'metrics' table if it doesn't exist
        session.sql("""
            CREATE TABLE IF NOT EXISTS ecs_schema.metrics (
                "@timestamp" TIMESTAMP_NTZ,
                "metricset.name" STRING,
                "metric.value" FLOAT,
                "attributes" VARIANT
            )
        """).collect()

        # Read new data from the metrics_stream
        metrics_df = session.table("metrics_stream")

        # Check if there is new data to process
        if metrics_df.count() > 0:
            # Transform and insert data into ecs_schema.metrics
            metrics_transformed = metrics_df.select(
                col("timestamp").alias("@timestamp"),
                col("metric_name").alias("metricset.name"),
                col("value").alias("metric.value"),
                parse_json(col("attributes")).alias("attributes")
            )

            metrics_transformed.write.mode("append").save_as_table("ecs_schema.metrics")
        else:
            print("No new metrics data to process.")

        # ***********************
        # Process Traces
        # ***********************

        # Create the target 'traces' table if it doesn't exist
        session.sql("""
            CREATE TABLE IF NOT EXISTS ecs_schema.traces (
                "trace.id" STRING,
                "span.id" STRING,
                "span.name" STRING,
                "span.start" TIMESTAMP_NTZ,
                "span.end" TIMESTAMP_NTZ,
                "span.duration" FLOAT,
                "attributes" VARIANT
            )
        """).collect()

        # Read new data from the traces_stream
        traces_df = session.table("traces_stream")

        # Check if there is new data to process
        if traces_df.count() > 0:
            # Calculate span duration
            traces_transformed = traces_df.with_column(
                "span.duration",
                datediff("milliseconds", col("start_time"), col("end_time"))
            ).select(
                col("trace_id").alias("trace.id"),
                col("span_id").alias("span.id"),
                col("name").alias("span.name"),
                col("start_time").alias("span.start"),
                col("end_time").alias("span.end"),
                col("span.duration"),
                parse_json(col("attributes")).alias("attributes")
            )

            traces_transformed.write.mode("append").save_as_table("ecs_schema.traces")
        else:
            print("No new traces data to process.")

        return 'Incremental transformation completed successfully.'
    except Exception as e:
        return 'Error: ' + str(e)
$$;

CREATE OR REPLACE TASK ecs_transform_task
WAREHOUSE = your_warehouse_name  -- Replace with your warehouse name
SCHEDULE = '1 MINUTE'
AS
CALL ecs_transform_incremental();

-- Enable the Task

use role accountadmin;
grant EXECUTE TASK on account to role oteltest;
use role oteltest;
ALTER TASK ecs_transform_task RESUME;

show tasks;
show alerts;

SELECT * from LOGIN_EVENTS;

-- monitor alerts & tasks

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp())))
ORDER BY SCHEDULED_TIME DESC;

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp())))
ORDER BY SCHEDULED_TIME DESC;