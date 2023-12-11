# Snowflake-Air-Quality-vs.-Traffic-Level
Data analytics project using snowflake to allow historical and real-time data ingestion using snowpipe.

### Data sources (both real-time and historical)
1. Air quality measurements: OpenAQ REST API
2. Traffic level measurements: TomTom Traffic Flow API

Location Information
1. Toronto Downtown (Air Monitor): (43.64399, -79.38859)
2. Massie, ON (Air Sensor): (44.45424702043515, -80.89603037601445)
3. Harbour Terrace, ON (Air Sensor): (43.63881659862974, -79.39156109746042)

| Location Name | Air Sensor Type | Air Level Parameters | Coordinates |
| ------------- | ------------- | ------------- | 
| Toronto Downtown  | Air Monitor  | Content Cell  | Content Cell  |
| Massie, ON  | Air Sensor  | Content Cell  | Content Cell  |
| Harbour Terrace, ON  | Air Sensor  | Content Cell  | Content Cell  |
| Wellington Place, ON | Air Sensor  | H (%), T (f), PM0.3 count (particles/cm³), T (c), PM5.0 count (particles/cm³), PM0.5 (particles/cm³), VOC (iaq), PM2.5 (µg/m³), PM2.5 count (particles/cm³), PM10 count (particles/cm³), PM1 (µg/m³), null (mb), PM1 count (particles/cm³), PM10 (µg/m³) | Content Cell  |


## Part 1: Historical Data Loading

Historical air sensor measurements and traffic information on the 
