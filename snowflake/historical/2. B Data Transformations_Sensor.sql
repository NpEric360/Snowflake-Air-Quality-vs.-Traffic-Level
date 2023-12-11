
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

--> set database
USE DATABASE AIRQUALITY_TRAFFIC_DATA;
---------------------------------

--1. Create a copy of the raw sensor data to transform

CREATE or REPLACE table airquality_traffic_data.air_quality.cleaned_sensor_data AS
SELECT * 
FROM airquality_traffic_data.air_quality.sensor_data;

--> Transformation A: Create Date and Time columns
    --A0. Create 2 new columns to extract Date and Time from DateTime column for easier averaging (daily/hourly)
ALTER table airquality_traffic_data.air_quality.cleaned_sensor_data
ADD column "date" date,
    column "hour" NUMBER;

    --A1. Update the new columns by extracting the date and time components of column datelocal
UPDATE airquality_traffic_data.air_quality.cleaned_sensor_data
SET "date" = DATE("datelocal"),
    "hour" = HOUR("datelocal");

    
-->Check

SELECT TOP 50 "date", "hour", "datelocal"
FROM airquality_traffic_data.air_quality.cleaned_sensor_data;

SELECT TOP 5 *
FROM airquality_traffic_data.air_quality.cleaned_sensor_data;

SELECT *
FROM airquality_traffic_data.air_quality.cleaned_sensor_data;
