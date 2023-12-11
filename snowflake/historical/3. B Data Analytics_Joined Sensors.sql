---------> This sheet contains the queries to join traffic, and air sensors together 

USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;
--------------------------------

--> set database
USE DATABASE AIRQUALITY_TRAFFIC_DATA;
---------------------------------

--------------------------------HOURLY

--- 1. View Sensor Data HOURLY and Traffic Data Hourly
SELECT SENSOR."date", SENSOR."hour", "parameter", "value", "avg_traffic_level", "kmh_below_free_flow"
FROM airquality_traffic_data.air_quality.cleaned_sensor_data SENSOR
LEFT JOIN airquality_traffic_data.traffic.hourly_traffic_data TRAFFIC ON SENSOR."date" = TRAFFIC."date" AND SENSOR."hour" = TRAFFIC."hour"
WHERE "parameter" LIKE '%nox%' --AND "value" < 25 AND SENSOR."date" <> '2023-11-30'
ORDER BY "date", "hour"
;

---------------------------------- FILTERING HIGH AVERAGE SENSOR DAYS, AND RUSH HOURS

-------------- SEARCH FOR HIGH AVERAGE TRAFFIC AND LOW AVERAGE TRAFFIC HOURS

CREATE OR REPLACE VIEW rush_hours AS
    SELECT "hour", avg("avg_traffic_level") as traffic                                                                                      
    FROM airquality_traffic_data.traffic.hourly_traffic_data
    GROUP BY "hour"
    HAVING traffic < 0.7;


SELECT *
FROM rush_hours;
------------------------------------

SELECT SENSOR."hour", AVG("value"), AVG("avg_traffic_level")
FROM airquality_traffic_data.air_quality.cleaned_sensor_data SENSOR
LEFT JOIN airquality_traffic_data.traffic.hourly_traffic_data TRAFFIC ON SENSOR."date" = TRAFFIC."date" AND SENSOR."hour" = TRAFFIC."hour"
WHERE "parameter" LIKE '%pm25%' AND SENSOR."hour" IN (SELECT "hour" FROM rush_hours) --AND "value" < 25 AND SENSOR."date" <> '2023-11-30'
GROUP BY SENSOR."hour"
UNION ALL

--- NON RUSH HOUR

SELECT SENSOR."hour", AVG("value"), AVG("avg_traffic_level")
FROM airquality_traffic_data.air_quality.cleaned_sensor_data SENSOR
LEFT JOIN airquality_traffic_data.traffic.hourly_traffic_data TRAFFIC ON SENSOR."date" = TRAFFIC."date" AND SENSOR."hour" = TRAFFIC."hour"
WHERE "parameter" LIKE '%pm25%' AND SENSOR."hour" NOT IN (SELECT "hour" FROM rush_hours) --AND "value" < 25 AND SENSOR."date" <> '2023-11-30'
GROUP BY SENSOR."hour"
;



