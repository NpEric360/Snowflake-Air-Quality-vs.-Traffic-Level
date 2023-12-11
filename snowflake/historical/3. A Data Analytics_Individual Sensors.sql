----- This sheet contains the queries to query traffic, and air sensors INDIVIDUALLY

USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

--> set database
USE DATABASE AIRQUALITY_TRAFFIC_DATA;


----------------------------------
--> AIR SENSOR
-->
-----------------------------------
--- 1. View air sensor parameter over entire time range
SELECT "datelocal","parameter", "value"
FROM airquality_traffic_data.air_quality.cleaned_sensor_data
WHERE "parameter" ILIKE '%nox%';

--- 2. Hourly Sensor value (not averaged)
SELECT "hour","parameter","value" 
FROM airquality_traffic_data.air_quality.cleaned_sensor_data
WHERE "parameter" LIKE '%nox%'
ORDER BY "hour" asc;

--- 3. Averaged Hourly Sensor Value
SELECT "hour","parameter",avg("value")
FROM airquality_traffic_data.air_quality.cleaned_sensor_data
WHERE "parameter" LIKE '%nox%'
GROUP BY "hour", "parameter"
ORDER BY "hour" asc;

--- 4. Find abnormally high daily average days
SELECT "date","parameter",avg("value")
FROM airquality_traffic_data.air_quality.cleaned_sensor_data
WHERE "parameter" LIKE '%pm25%'
GROUP BY "date", "parameter"
HAVING avg("value") > 15
ORDER BY "date" asc;

----------------------------------
--> TRAFFIC LEVELS
-->
-----------------------------------

--- 1. Traffic speeds/diff/levels over time
SELECT "datetime", "current_speed" as "current_speed (KMH)","free_flow_speed" as "free_flow_speed (KMH)", "traffic_level" as "traffic_level (%)", "free_flow_speed"-"current_speed" as "KMH Below Free Flow Speed"
FROM airquality_traffic_data.traffic.cleaned_trafic_data;


---- 2. Average Hourly Traffic Levels

SELECT hour("time") as "hour", avg("current_speed") as "avg_current_speed (KMH)",avg("free_flow_speed") as "avg_free_flow_speed (KMH)", avg("traffic_level") as "avg_traffic_level (%)", avg("free_flow_speed"-"current_speed") as "Avg_KMH Below Free Flow Speed"
FROM airquality_traffic_data.traffic.cleaned_trafic_data
GROUP BY hour("time")
ORDER BY hour("time") ASC;







