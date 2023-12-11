from Traffic_API import latest_traffic
from Airsensor_API import latest_air_sensor
import pandas as pd
import os 

import ast
import schedule, time
from datetime import datetime
import pytz

api_key = ''
csv_file = 'location_sensor_data.csv'

## Create a table containing Location, Air Sensor ID, Coordinates for both api's

#Initialize or open the previously saved location data
def open_csv():
    
    if os.path.exists(csv_file):
        locdata = pd.read_csv(csv_file)
        return locdata
    else:
        default_date = '2020-12-09 14:00'
        cols = ['Location_Name', 'Sensor_Type', 'Air_Sensor_ID', 'Coordinates', 'Recent_Air_Timestamp']

        loc_info = [
            ['Toronto_Downtown', 'Monitor', 7570, '(43.64399, -79.38859)', default_date],
            ['Massie', 'Sensor', 1191546, '(44.45424702043515, -80.89603037601445)', default_date],
            ['Harbour Terrace-1', 'Sensor', 221512, '(43.63881659862974, -79.39156109746042)', default_date],
            ['Wellington Place (elev: 90m)', 'Sensor', 1092284, '(43.64482224743071, -79.39476761576265)', default_date]
        ]
        locdata = pd.DataFrame(loc_info, columns=cols)
        locdata.to_csv(csv_file,index=False)
        return locdata

def main():
    locdata = open_csv()

    #main loop
    for i in range(len(locdata)):
        print('################ Main loop ', i)
        parameters = locdata.iloc[i].values #Locatio5, Sensor Type, Sensor ID, Coordinates, Most recent sensor timestamp
        try:
            #coordinates are read as a string, and is covnerted back to a tuple using ast
            
            traffic_response = latest_traffic(parameters[0], ast.literal_eval(parameters[3]), api_key)
            air_response = latest_air_sensor(locdata, parameters[2])
            
            #print("TR = ",traffic_response)
            #print("AIR = ",latest_air_sensor(locdata, 7570))
        except Exception as e:
            print(e)

    current_time = datetime.now(pytz.timezone('EST'))
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M')
    print('###########  Finished api calls at ', formatted_time, ' ####################')
    #once this function is existed, save the current pandas dataframe as a .csv file for future runs
    locdata.to_csv(csv_file,index=False)

#main()
schedule.every().hour.at(":00").do(main)
schedule.every().hour.at(":30").do(main)
while True:
    schedule.run_pending()
    time.sleep(60)
#Create a scheduler to run traffic every 30 mins, air sensor every 1 hour, etc