--Step 1: Create the logs_of_last_10_mins Table
--First, ensure that the target table logs_of_last_10_mins exists with the same structure as ecs_schema.logs. If it doesn't exist, create it:

use role oteltest;
CREATE OR REPLACE TABLE logs_of_last_10_mins LIKE ecs_schema.logs;
--This statement creates logs_of_last_10_mins with the same schema as ecs_schema.logs but without any data.

--Step 2: Create the Stored Procedure
--We'll create a stored procedure named refresh_logs_of_last_10_mins that:

--Truncates the logs_of_last_10_mins table.
--Inserts logs from the last 10 minutes into the table.
--Stored Procedure Code:
CREATE OR REPLACE PROCEDURE refresh_logs_of_last_10_mins()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Truncate the target table
  TRUNCATE TABLE logs_of_last_10_mins;

  -- Insert logs from the last 10 minutes
  INSERT INTO logs_of_last_10_mins
  SELECT *
  FROM ecs_schema.logs
  WHERE "@timestamp" >= DATEADD('minute', -10, CURRENT_TIMESTAMP());

  RETURN 'Logs refreshed successfully.';
END;
$$;


--The TRUNCATE TABLE command removes all data from logs_of_last_10_mins.
--The INSERT INTO command copies logs from the last 10 minutes.
--DATEADD('minute', -10, CURRENT_TIMESTAMP()) computes the timestamp 10 minutes before the current time.
--CURRENT_TIMESTAMP() returns the current timestamp in UTC by default.

--Step 3: Create the Task
--Create a task named refresh_logs_task that calls the stored procedure on a schedule.

--Example Task Creation:

CREATE OR REPLACE TASK refresh_logs_task
WAREHOUSE = your_warehouse_name  -- Replace with your warehouse name
SCHEDULE = 'USING CRON */10 * * * * UTC'  -- Runs every 10 minutes
AS
CALL refresh_logs_of_last_10_mins();




--WAREHOUSE: Replace your_warehouse_name with the name of your compute warehouse.
--SCHEDULE: 'USING CRON */10 * * * * UTC' schedules the task to run every 10 minutes.
--AS: Specifies the SQL command to execute, which is the stored procedure call.

--Start the Task:
--Step 4: Enable Task Scheduling (If Not Already Enabled)
--Ensure that task scheduling is enabled in your Snowflake account:
ALTER ACCOUNT SET TASK_SCHEDULING = TRUE;
ALTER TASK refresh_logs_task RESUME;


--Testing the Stored Procedure and Task
--Manually Test the Stored Procedure:

--Before relying on the task, you can manually test the stored procedure:

CALL refresh_logs_of_last_10_mins();
--Check the Data in logs_of_last_10_mins:
--After running the stored procedure, verify that the data is as expected:
SELECT * FROM logs_of_last_10_mins;
--Monitor the Task Execution:

--You can check the task's history and status:
SHOW TASKS LIKE 'refresh_logs_task';
--To view the task's execution history:

SELECT *
FROM TABLE(information_schema.task_history())
WHERE name = 'REFRESH_LOGS_TASK'
ORDER BY scheduled_time DESC;

--Considerations
--Warehouse Selection:

--Ensure the warehouse specified in the task (your_warehouse_name) is suitable for the workload.
--The warehouse must be active or set to auto-resume for the task to run.

--Time Zones:

--The CURRENT_TIMESTAMP() function returns the current timestamp in UTC by default.
--If your @timestamp data is in a different time zone, adjust the timestamp calculations accordingly.

--Permissions:

--The role executing these commands must have sufficient privileges:
--CREATE PROCEDURE, CREATE TASK, TRUNCATE TABLE, INSERT permissions on the relevant schemas and tables.
--USAGE privilege on the warehouse.

--Error Handling:

--For production use, consider enhancing the stored procedure with error handling using EXCEPTION blocks.
--Log any errors or send notifications if needed.

--Data Volume:

--If the logs table contains a large volume of data, ensure that the query is optimized.
--Consider adding appropriate indexes or clustering keys on @timestamp if necessary.

--Alternative: Use Streams and Tasks (Optional):

--If you prefer not to truncate and reload data, you can use Snowflake Streams to track changes and Tasks to process them incrementally.
--This approach is more efficient for large datasets but is more complex to set up.
--Example: Adjusting for Time Zone (If Needed)
--If your @timestamp column is in a specific time zone, you can adjust the DATEADD calculation.

--Example for Pacific Time:


-- Adjust the current timestamp to Pacific Time
INSERT INTO logs_of_last_10_mins
SELECT *
FROM ecs_schema.logs
WHERE "@timestamp" >= DATEADD('minute', -10, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()));

--However, it's generally recommended to store and process timestamps in UTC to avoid confusion.

--Additional Enhancements (Optional)
--Add Error Handling to the Stored Procedure:


CREATE OR REPLACE PROCEDURE refresh_logs_of_last_10_mins()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  err_msg STRING;
BEGIN
  -- Truncate the target table
  TRUNCATE TABLE logs_of_last_10_mins;

  -- Insert logs from the last 10 minutes
  INSERT INTO logs_of_last_10_mins
  SELECT *
  FROM ecs_schema.logs
  WHERE "@timestamp" >= DATEADD('minute', -10, CURRENT_TIMESTAMP());

  RETURN 'Logs refreshed successfully.';
EXCEPTION
  WHEN OTHER THEN
    err_msg := 'Error: ' || SQLERRM;
    RETURN err_msg;
END;
$$;


--Uses an EXCEPTION block to catch any errors.
--Returns an error message if something goes wrong.

--Conclusion
--By following these steps, you have set up a Snowflake task that:

--Periodically (every 10 minutes) refreshes the logs_of_last_10_mins table with the latest logs from the ecs_schema.logs table.
--Ensures that the data in logs_of_last_10_mins always reflects the last 10 minutes of logs.
--Automates the process using a stored procedure and a task, eliminating manual intervention.