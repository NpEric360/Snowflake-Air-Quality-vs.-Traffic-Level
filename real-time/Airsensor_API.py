#Libraries
import requests
import json
from datetime import datetime
import pandas as pd


import boto3
from datetime import datetime
import pytz
import ast
from io import StringIO

key_id = ''
secret_key = ''
bucket_name = 'snowpipe-project-1'

#Step 1: Connect to AWS S3 bucket using AWS credentials and upload csv files
def write_to_s3(csv_string, file_name):
    s3_client = boto3.client("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)

    # Upload CSV file to S3
    s3_client.put_object(Bucket=bucket_name, Key=f'air-sensor-measurements/{file_name}', Body=(csv_string.getvalue()))
    print(f"CSV air data uploaded to {bucket_name}/air-sensor-measurements/{file_name}")

#Step 2: convert the UTC timestamp from API response to EST local to make traffic api measurements

def convert_utc_to_et(utc_timestamp):
    # Define UTC and EST timezones
    utc_timezone = pytz.timezone('UTC')
    et_timezone = pytz.timezone('US/Eastern')
    
    # Convert UTC string to datetime object
    utc_time = datetime.strptime(utc_timestamp, '%Y-%m-%dT%H:%M:%S%z')
    # Set UTC timezone to the UTC time
    utc_time = utc_time.astimezone(utc_timezone)
    # Convert UTC time to Eastern Time
    et_time = utc_time.astimezone(et_timezone)
    return et_time.strftime('%Y-%m-%d %H:%M')

#Step 3: Compare two dates
def date_compare(date_str1,date_str2):
    #Convert date strings to datetime objects
    date1 = datetime.strptime(date_str1, '%Y-%m-%d %H:%M')
    date2 = datetime.strptime(date_str2, '%Y-%m-%d %H:%M')
    if date2 > date1:
        return True
    else:
        return False

#Step 4: Generate a filename in the format: airsensor_date_location.csv

def airsensor_filename(air_response):
    utc_time = air_response['results'][0]['measurements'][0]['lastUpdated']
    est_time = convert_utc_to_et(utc_time)
    location = air_response['results'][0]['location'].replace(' ','_')
    filename = 'airsensor_' + est_time + '_' + location +'.csv'
    return filename, est_time


#Integrate the location/sensor data and the measurements into a pandas dataframe -> save as a .csv in the memory buffer
def format_air_response_csv(locdata_row, est_time, measurements):
    #convert the nested json to a flattened csv to allow easier ingestion for snowpipe 
    location_id, location, sensor_type, coordinates = locdata_row[0][2], locdata_row[0][0], locdata_row[0][1], ast.literal_eval(locdata_row[0][3])
    formatted_data = {
        'locationid' : location_id,
        'location': location,
        'datelocal': est_time,
        'latitude': coordinates[0],
        'longitude': coordinates[1],
        'sensortype': sensor_type
    }
    #GOAL: Create a row with the formatted data and measurement for each individual measured parameter in api response; i.e. o3, pm25
    #Create a df with duplicate rows of formatted_data with length of measurements

    #Convert the format data to a dataframe and duplicate rows to make the size of the measurements dataframe
    format_df = pd.DataFrame([formatted_data])
    format_df_repeat = pd.concat([format_df]*len(measurements))
    format_df_repeat.reset_index(inplace = True, drop = True) #reset the indices: the duplicated rows will all have the same index = 0, which breaks pd.cat

    measurement_df = pd.DataFrame(measurements)
    combined_df = pd.concat([format_df_repeat, measurement_df],axis = 1)
    #Convert this df to a csv and save it in memory 
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index = False)

    return csv_buffer

###AIR SENSOR API

def latest_air_sensor(locdata, location_id):
    url = f"https://api.openaq.org/v2/latest/{location_id}?limit=100&page=1&offset=0&sort=asc"
    headers = {"accept": "application/json"}
    try:
        response =requests.get(url, headers=headers)
        if response.status_code == 200:
            json_string = json.loads(response.text)
            file_name, est_time = airsensor_filename(json_string)
            measurements = (json_string['results'][0]['measurements'])
            formatted_response = format_air_response_csv(locdata[locdata['Air_Sensor_ID'] == location_id].values, est_time, measurements)
            #check if the current measurement timestamp is different from the most recent measurement timestamp:
                #Retrieve the specific 'recent_air_timestamp' of the matching row
            recent_timestamp = locdata[locdata['Air_Sensor_ID'] == location_id]['Recent_Air_Timestamp'].iloc[0]

            if date_compare(recent_timestamp, est_time):
                #update the dataframe by locating the index of the row matching the current location_id, and filter for the column 'Recent_Air_Timestamp'
                indices = locdata.index[(locdata['Air_Sensor_ID'] == location_id) & (locdata['Recent_Air_Timestamp'] == recent_timestamp)].tolist()
                locdata.at[indices[0], 'Recent_Air_Timestamp'] = est_time
                print("New air measurement created.")
                write_to_s3(formatted_response, file_name)
            else:
                print('No new air measurements found at ', est_time)
        
        else:
            return ("Error: ", response.status_code)
    except Exception as e:
        print(e)
