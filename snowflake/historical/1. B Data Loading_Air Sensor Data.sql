---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

--> set database
USE DATABASE AIRQUALITY_TRAFFIC_DATA;
---------------------------------


---> query the Stage to find the Menu CSV file
LIST @airquality_traffic_data.public.blob_stage/sensor_data/;


-- view the inferred schema from specific file in s3 bucket: note file name is case sensitive 

-- Sensor 1: Harbour Terrace
SELECT * FROM TABLE(
    INFER_SCHEMA(
     LOCATION=>'@airquality_traffic_data.public.blob_stage/sensor_data/measurements_harbour_terrace.csv',
     FILE_FORMAT=>'CSV_INFER'
    )
 );

-- Sensor 2: Wellington Place
SELECT * FROM TABLE(
    INFER_SCHEMA(
     LOCATION=>'@airquality_traffic_data.public.blob_stage/sensor_data/measurements_wellington_place.csv',
     FILE_FORMAT=>'CSV_INFER'
    )
 );

 
---create a table based on the inferred schema, if it doesn't already exist

CREATE OR REPLACE TABLE airquality_traffic_data.air_quality.sensor_data
--CREATE OR REPLACE TABLE airquality_traffic_data.air_quality.sensor_data
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
         LOCATION=>'@airquality_traffic_data.public.blob_stage/sensor_data/measurements_harbour_terrace.csv',
         FILE_FORMAT=>'CSV_INFER'
        )
      ));
      
-- alter the inferred locationId column to fit larger IDs,and round value column to four decimal places
ALTER TABLE airquality_traffic_data.air_quality.sensor_data
MODIFY "locationid" NUMBER(10,0);

--check table is empty

---> confirm the empty Menu table exists
SELECT * FROM airquality_traffic_data.air_quality.sensor_data;

---> query the Stage to find the csv files
LIST @airquality_traffic_data.public.blob_stage/sensor_data/;

---> copy the .csv sensor measurements from the stage
COPY INTO airquality_traffic_data.air_quality.sensor_data
FROM @airquality_traffic_data.public.blob_stage/sensor_data/measurements_harbour_terrace.csv
file_format = 'CSV_skip_header';


COPY INTO airquality_traffic_data.air_quality.sensor_data
FROM @airquality_traffic_data.public.blob_stage/sensor_data/measurements_wellington_place.csv
file_format = 'CSV_skip_header';


---> how many rows are in the table?
SELECT COUNT(*) AS row_count FROM airquality_traffic_data.air_quality.sensor_data;

---> what do the top 10 rows look like?
SELECT TOP 10 * FROM airquality_traffic_data.air_quality.sensor_data;

--> check both locations
SELECT "location"
FROM airquality_traffic_data.air_quality.sensor_data
GROUP BY "location";

--> PM25 levels on 2023-12-02

SELECT '2023-12-02' AS "dateLocal", AVG("value") AS avg_value
FROM airquality_traffic_data.air_quality.sensor_data
WHERE "parameter" LIKE '%pm25%' AND "dateLocal" LIKE '%2023-12-02%';


--> Sample 2 rows from each location value

SELECT TOP 2 * FROM airquality_traffic_data.air_quality.sensor_data
WHERE "location" LIKE '%Harbour%'
UNION ALL
SELECT TOP 2 * FROM airquality_traffic_data.air_quality.sensor_data
WHERE "location" LIKE '%Well%'






