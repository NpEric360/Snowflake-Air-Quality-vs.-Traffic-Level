--> Script purpose
--  1. Create DB, schemas
--  2. Create


---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE airquality_traffic_data;
---> create the Schemas
CREATE OR REPLACE SCHEMA airquality_traffic_data.air_quality;
CREATE OR REPLACE SCHEMA airquality_traffic_data.traffic;

CREATE OR REPLACE STAGE airquality_traffic_data.public.blob_stage
url = 's3://snowflake-traffic-data/'
CREDENTIALS = (
    AWS_KEY_ID = ''
    AWS_SECRET_KEY = ''
)

-- Create a file format that sets the file type as CSV and parses the header
CREATE OR REPLACE FILE FORMAT CSV_INFER
  TYPE = csv
  PARSE_HEADER = TRUE;

-- Create a file format that skips the first header: used for data copying
CREATE OR REPLACE FILE FORMAT CSV_skip_header
  TYPE = csv
  SKIP_HEADER = 1;




