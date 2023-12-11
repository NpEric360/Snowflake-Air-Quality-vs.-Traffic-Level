
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

--> set database
USE DATABASE AIRQUALITY_TRAFFIC_DATA;
---------------------------------



--PART 1. Create a copy of the raw sensor data to transform

CREATE or REPLACE table airquality_traffic_data.traffic.cleaned_trafic_data AS
SELECT * 
FROM airquality_traffic_data.traffic.traffic_data;

--> Transformation A: Create Date and Time columns
    --A0. Create 2 new columns to extract Date and Time from DateTime column for easier averaging (daily/hourly)
ALTER table airquality_traffic_data.traffic.cleaned_trafic_data
ADD column "date" date,
    column "time" time,
    column "traffic_level" NUMBER(5,2);

    --A1. Update the new columns by extracting the date and time components of column datelocal
UPDATE airquality_traffic_data.traffic.cleaned_trafic_data
SET "date" = DATE("datetime"),
    "time" = TIME("datetime"),
    "traffic_level" = ROUND("current_speed"/"free_flow_speed",2);

    
-->Check Transformations
-- A0
SELECT TOP 50 "date", "time", "datetime"
FROM airquality_traffic_data.traffic.cleaned_trafic_data;
-- A1: 
SELECT TOP 5 "current_speed", "free_flow_speed", "traffic_level"
FROM airquality_traffic_data.traffic.cleaned_trafic_data;

--view top 5 rows
SELECT TOP 5 *
FROM airquality_traffic_data.traffic.cleaned_trafic_data;

--Simple speed check
SELECT TOP 5 "current_speed", "free_flow_speed", "traffic_level"
FROM airquality_traffic_data.traffic.cleaned_trafic_data
WHERE "current_speed" > "free_flow_speed";

--PART 2. Create a new table where the traffic data is averaged by hour to match the hourly data in sensor data

--Create a traffic data table where levels are averaged hourly
CREATE OR REPLACE TABLE airquality_traffic_data.traffic.hourly_traffic_data AS
    SELECT "date", HOUR("time") as "hour", avg("current_speed") as "avg_current_speed", avg("free_flow_speed") as "avg_free_flow_speed", avg("traffic_level") as "avg_traffic_level", avg("free_flow_speed") - avg("current_speed") as "kmh_below_free_flow", avg("road_closure") as "avg_road_closure"
    FROM airquality_traffic_data.traffic.cleaned_trafic_data
    GROUP BY "date", HOUR("time")
    ORDER BY "date" DESC, HOUR("time") ASC;

--View new table
SELECT *
FROM airquality_traffic_data.traffic.hourly_traffic_data;
