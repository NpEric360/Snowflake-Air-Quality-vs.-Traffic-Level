"""
Purpose: Cast all csv header to lower case to prevent case sensitive queries
"""


import pandas as pd
import os

file_dir = os.path.join(os.getcwd(),'s3_dir','sensor_data')
files = os.listdir(file_dir)

def fix_headers(csv_file):
    data = pd.read_csv(os.path.join(file_dir,csv_file))
    #Cast csv headers to lowercase
    data.columns = map(str.lower, data.columns)
    data.to_csv(os.path.join(file_dir,i), index = False)

for i in files:
    fix_headers(i)
