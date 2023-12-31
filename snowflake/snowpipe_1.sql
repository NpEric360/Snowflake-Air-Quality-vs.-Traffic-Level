---> Goal: Set up snowpipe

--> Script purpose
--  1. Create DB, tables schemas, file formats, stages
--  2. Create snowpipes for traffic and sensor data


---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------
USE SCHEMA SP_AIRQUALITY_TRAFFIC_DATA.public;

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE SP_airquality_traffic_data;

-- Create a file format that skips the first header: used for data copying
CREATE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1; 


create schema traffic;
create schema airsensor;


create or replace TABLE SP_AIRQUALITY_TRAFFIC_DATA.AIRSENSOR.SENSOR_DATA (
	"locationid" NUMBER(10,0),
	"location" VARCHAR(16777216),
	"datelocal" TIMESTAMP_NTZ(9),
    "latitude" NUMBER(7,5),
	"longitude" NUMBER(7,5),
    "sensortype" VARCHAR(16777216),
    "parameter" VARCHAR(16777216),
	"value" NUMBER(10,3),
	"lastUpdated" TIMESTAMP_NTZ(9),
	"unit" VARCHAR(16777216)	
);


create or replace TABLE SP_AIRQUALITY_TRAFFIC_DATA.TRAFFIC.TRAFFIC_DATA (
	"datetime" TIMESTAMP_NTZ(9),
    "location" VARCHAR(1677216),
	"frc" VARCHAR(16777216),
	"currentspeed" NUMBER(2,0),
	"freeflowspeed" NUMBER(2,0),
	"currenttraveltime" NUMBER(4,0),
	"freeflowtraveltime" NUMBER(3,0),
	"confidence" NUMBER(1,0),
	"roadclosure" NUMBER(1,0)
);

CREATE OR REPLACE STAGE sp_airquality_traffic_data.public.traffic_s3_stage
url = 's3://snowpipe-project-1/traffic-measurements'
CREDENTIALS = (
    AWS_KEY_ID = ''
   AWS_SECRET_KEY = ''
);

CREATE OR REPLACE STAGE sp_airquality_traffic_data.public.airsensor_s3_stage
url = 's3://snowpipe-project-1/air-sensor-measurements'
CREDENTIALS = (
    AWS_KEY_ID = ''
   AWS_SECRET_KEY = ''
);

LIST @sp_airquality_traffic_data.public.airsensor_s3_stage;

-- Create the Snowpipe
CREATE OR REPLACE PIPE sp_airquality_traffic_data.public.traffic_snowpipe 
    AUTO_INGEST = TRUE 
    AS
    COPY INTO sp_airquality_traffic_data.traffic.traffic_data
    FROM @sp_airquality_traffic_data.public.traffic_s3_stage
    FILE_FORMAT = (TYPE = 'JSON')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sp_airquality_traffic_data.public.airsensor_snowpipe 
    AUTO_INGEST = TRUE 
    AS
    COPY INTO sp_airquality_traffic_data.airsensor.sensor_data
    FROM @sp_airquality_traffic_data.public.airsensor_s3_stage
    FILE_FORMAT = 'CSV_FORMAT'
;

show pipes;
describe pipe sp_airquality_traffic_data.public.airsensor_snowpipe;

--select SYSTEM$PIPE_STATUS('sp_airquality_traffic_data.public.traffic_snowpipe');
select SYSTEM$PIPE_STATUS('sp_airquality_traffic_data.public.airsensor_snowpipe');
--SELECT CURRENT_REGION();
