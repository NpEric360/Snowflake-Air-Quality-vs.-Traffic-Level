'''
This contains the functions that calls the traffic data api, extracts useful components, and uploads to s3 bucket

'''

#Libraries
import requests
import json
from datetime import datetime
import pandas as pd
import boto3

api_key = ''
key_id = ''
secret_key = ''
bucket_name = 'snowpipe-project-1'


#Step 1: Write json to s3 bucket
def write_to_s3(json_string, file_name):
    #Access S3
    try:
        s3_client = boto3.client("s3", aws_access_key_id = key_id, aws_secret_access_key = secret_key)
        # Upload JSON data to S3
        s3_client.put_object(Bucket=bucket_name, Key=f'traffic-measurements/{file_name}', Body=json.dumps(json_string))
        print(f"JSON data uploaded to {bucket_name}/traffic-measurements/{file_name}")
    except Exception as e:
        print(e)


def format_input_data(location, api_response):
    ### Helper function to remove unnecessary keys in API response & Returns a clean dictionary
    #A. remove unwanted keys
    unwanted_keys = ['coordinates','@version']
    for key in unwanted_keys:
        del api_response['flowSegmentData'][key]
    
    filtered_response = api_response['flowSegmentData']
    #B. Insert datetime of current api call
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M')
    filtered_response['datetime'] = formatted_datetime
    print("Recorded traffic data at ", formatted_datetime)

    cleaned_traffic_response = {'Location':location}
    for key in filtered_response:
        cleaned_traffic_response[key.lower()] = filtered_response[key]
    return cleaned_traffic_response

def traffic_filename(location, datetime):
    #print(location,datetime)
    date_time = datetime.split() #datetime is date+time
    filename = 'traffic_'+str(date_time[0])+'_'+str(date_time[1])+'_'+str(location)+'.json'
    return filename

def latest_traffic(location, coordinates, api_key):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point={coordinates[0]}%2C{coordinates[1]}&unit=KMPH&thickness=1&openLr=false&key={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            response_json = json.loads(response.text)
            cleaned_json = format_input_data(location, response_json)
            filename = traffic_filename(location,cleaned_json['datetime'])
            write_to_s3(cleaned_json, filename)
            return cleaned_json
    except Exception as e:
        print(e)

#print(latest_traffic('Toronto_Downtown',(43.64399, -79.38859),api_key))
