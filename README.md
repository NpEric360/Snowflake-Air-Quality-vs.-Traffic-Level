# Snowflake-Air-Quality-vs.-Traffic-Level
Data analytics project using snowflake to allow historical and real-time data ingestion using snowpipe.

### Data sources (both real-time and historical)
1. Air quality measurements: OpenAQ REST API
https://docs.openaq.org/docs
2. Traffic level measurements: TomTom Traffic Flow API
https://developer.tomtom.com/traffic-api/api-explorer

Location Information

| Location Name | Air Sensor Type |
| ------------- | ------------- |
| Toronto Downtown  | Air Monitor  | 
| Massie, ON  | Air Sensor  | 
| Harbour Terrace, ON  | Air Sensor  |
| Wellington Place, ON | Air Sensor  | 

### Air Sensors and monitor measurements include: 
1. Monitor: NO (ppm), NOx (ppm), NO₂ (ppm), PM2.5 (µg/m³), O₃ (ppm)
2. Air Sensor: PM2.5 count (particles/cm³), PM5.0 count (particles/cm³), T (f), PM1 (µg/m³), PM0.3 count (particles/cm³), PM10 (µg/m³), PM0.5 (particles/cm³), PM1 count (particles/cm³), PM10 count (particles/cm³), PM2.5 (µg/m³), H (%)

## Part 1: Historical Data Loading

1. Historical air sensor measurements and traffic CSV files are uploaded to an S3 bucket and are copied into Snowflake tables. Data transformations such as filtering and averaging are performed before the air sensor and traffic datasets are joined to be analyzed.
2. There are no traffic datasets that contain hourly data, so I created scripts to store real-time traffic data over time. An alternative could have been scraping google maps navigation data, however, those are approximate calculations and not as accurate as real-time data.

### Relevant files: 
1. Snowflake SQL Worksheets: /Snowflake/historical
2. Python Scripts/Files: /Historical

## Part 2: Real-time Data Ingestion using Snowpipe

1. Two snowpipes are created to auto-ingest uploaded air sensor data (.csv) and traffic data (.json) directly from their respective API calls.
2. In the /real-time directory, there are airsensor_api.py and traffic_api.py. These two files contain functions that call the api's and perform the necessary transformations to extract and format the desired parameters. If the api responses are unique and are not duplicates of previous measurements, they are uploaded to the target s3 bucket.
3. /real-time/API_scheduler.py creates or opens the location table which contains the location, sensor_id, coordinates, and most recent measurement timestamp of the locations that are being surveyed. This script also calls the api functions from the other two files every half-hour, and automates the pipeline when combined with the auto-ingesting snowpipes.

   
