# Snowflake-Air-Quality-vs.-Traffic-Level
Data analytics project using snowflake to allow historical and real-time data ingestion using snowpipe.

### Data sources (both real-time and historical)
1. Air quality measurements: OpenAQ REST API
2. Traffic level measurements: TomTom Traffic Flow API

Location Information
1. Toronto Downtown (Air Monitor): (43.64399, -79.38859)
2. Massie, ON (Air Sensor): (44.45424702043515, -80.89603037601445)
3. Harbour Terrace, ON (Air Sensor): (43.63881659862974, -79.39156109746042)

| Location Name | Air Sensor Type |
| ------------- | ------------- |
| Toronto Downtown  | Air Monitor  | 
| Massie, ON  | Air Sensor  | 
| Harbour Terrace, ON  | Air Sensor  |
| Wellington Place, ON | Air Sensor  | 


## Part 1: Historical Data Loading

Historical air sensor measurements and traffic information on the 
