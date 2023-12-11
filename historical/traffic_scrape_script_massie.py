import requests
import json
from datetime import datetime
import sqlite3
import schedule
import time
#Keys

api_key = ''
url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point=44.45424702043515%2C-80.89603037601445&unit=KMPH&thickness=1&openLr=false&key={api_key}"

#Formatting Functions
#A. Remove unnecssary keys in API response
def remove_key_from_dict(input_dict, keys_to_remove):
    return {key: value for key, value in input_dict.items() if key not in keys_to_remove}
#B. Convert API json response to tuple, include current date time
def format_input_data(api_response):
    clean_dict = remove_key_from_dict(api_response['flowSegmentData'],['coordinates','@version'])
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M')
    
    current_data = []
    for value in clean_dict.items():
        current_data.append(value[1])
    current_data.insert(0,formatted_datetime)

    return tuple(current_data)
    

def main():
    #Call API
    
    response = requests.get(url)
    if response.status_code == 200:
        print('Pass')
        response_json = json.loads(response.text)
        current_data = format_input_data(response_json)
        #Insert current_data to sqllite db
        cursor.execute(f"INSERT INTO Traffic_Conditions (datetime,frc,current_speed,free_flow_speed,current_travel_time,free_flow_travel_time,confidence,road_closure) VALUES (?, ?, ?, ?, ?, ?, ? ,?)", current_data)
        conn.commit()
        print(f"Wrote {current_data} at {current_data[0]}")


# Connect to SQLITE DB and insert values on a schedule (every hour and half hour)
conn = sqlite3.connect('tomTom_data_massie.db')
cursor = conn.cursor()

#schedule.every(30).minutes.do(main)
schedule.every().hour.at(":00").do(main)
schedule.every().hour.at(":30").do(main)
while True:
    schedule.run_pending()
    time.sleep(1)  

conn.close()