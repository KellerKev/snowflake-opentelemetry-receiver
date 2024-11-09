use role accountadmin;

CREATE OR REPLACE WAREHOUSE otelwh WITH WAREHOUSE_SIZE='X-SMALL';

create role oteltest;

grant usage on WAREHOUSE otelwh  to role oteltest;
grant operate on  WAREHOUSE otelwh to role oteltest;

grant create database on account to role oteltest;


grant role oteltest to user xxx;

use role oteltest;



create database otel;
create schema otelschema;

CREATE or replace TABLE metrics (
      timestamp TIMESTAMP_NTZ,
      metric_name STRING,
      value DOUBLE,
      attributes VARCHAR
);

CREATE or replace TABLE logs (
      timestamp TIMESTAMP_NTZ,
      log_level STRING,
      message STRING,
      attributes varchar
);


CREATE or replace TABLE  traces (
      trace_id STRING,
      span_id STRING,
      name STRING,
      start_time TIMESTAMP_NTZ,
      end_time TIMESTAMP_NTZ,
      attributes varchar
);

CREATE IMAGE REPOSITORY IF NOT EXISTS oteltestimages;

SHOW IMAGE REPOSITORIES IN SCHEMA;

--before uploading image, if you use a username/password to authenticate and MFA is enabled for your user, you better disable it
-- before uploading for a bit so you dont get locked out and receive 10000 MFA messages:

USE ROLE ACCOUNTADMIN;
--alter user xxxx set mins_to_bypass_mfa=10
-- use role oteltest;


GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE oteltest;

CREATE  COMPUTE POOL IF NOT EXISTS OTEL_COMPUTE_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_S
  AUTO_RESUME = true;

  DESCRIBE COMPUTE POOL OTEL_COMPUTE_POOL;
  
grant usage on compute pool OTEL_COMPUTE_POOL to role oteltest;
grant operate on compute pool OTEL_COMPUTE_POOL to role oteltest;
grant monitor on compute pool OTEL_COMPUTE_POOL to role oteltest;
grant modify on compute pool OTEL_COMPUTE_POOL to role oteltest;
grant ownership on compute pool OTEL_COMPUTE_POOL to role oteltest;

use role oteltest;


  CREATE SERVICE otel_service
  IN COMPUTE POOL OTEL_COMPUTE_POOL
  MIN_INSTANCES=1
  MAX_INSTANCES=1
  FROM SPECIFICATION
  $$
   spec:
     containers:
     - name: "otel"
       image: "/otel/otelschema/oteltestimages/otel-image:latest"
     env:
       SNOWFLAKE_DATABASE:"otel"
       SNOWFLAKE_WAREHOUSE:"otelwh"
       SNOWFLAKE_SCHEMA:"otelschema"
       SPCS: "True"
          
     endpoints:
     - name: otelhttp
       port: 4318
       public: true
  $$;

  
describe service otel_service;
SHOW SERVICE CONTAINERS IN SERVICE otel_service;
SHOW IMAGES IN IMAGE REPOSITORY oteltestimages;


CALL SYSTEM$GET_SERVICE_LOGS('otel_service', '0', 'otel', 1000);
SHOW ENDPOINTS IN SERVICE otel_service;

select * from metrics;
select * from logs;
select * from traces;
