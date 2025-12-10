import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime 
import pytz

log_file = "log_file.txt"
target_file = "transformed_data.csv"
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

#Extract file from csv

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

#Extraxt from json

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

#Exract file from XML

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['name','height','weight'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        dataframe = pd.concat([dataframe,pd.DataFrame([{'name':name,'height':height,'weight':weight}])],ignore_index=True)
    return dataframe

# Call files with relevent functions

# Create an empty file

def extract():
    extracted_data = pd.DataFrame(columns= ['name','height','weight'])

    # process csv file
    for csvfile in glob.glob('project/*.csv'):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_csv(csvfile))],ignore_index = True)
    # process json file
    for jsonfile in glob.glob('project/*.json'):
            extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_json(jsonfile))],ignore_index = True)
    # process xml file
    for xmlfile in glob.glob('project/*.xml'):
            extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_xml(xmlfile))],ignore_index = True)
    return extracted_data      

# Till here Extract completes


def transform(data):
    #convert height in inches
    data['height'] = pd.to_numeric(data['height'], errors='coerce')
    data['height'] = round(data.height* 0.0254, 2)
    #covert weight to kg
    data['weight'] = pd.to_numeric(data['weight'], errors='coerce')
    data['weight'] = round(data.weight * 0.45359237,2)

    return data

# Transforation complete

# Loading and logging

def load_data(target_file,transformed_data):
     transformed_data.to_csv(target_file)

# Log message

def log_progress(message):
     timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second 
     now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
     now_ist = now_utc.astimezone(IST_TIMEZONE)
     
     timestamp = now_ist.strftime(timestamp_format)
     with open(log_file,"a") as f:
          f.write(timestamp + ',' + message + '\n')

# Log initialization
log_progress('ETL job started')

# Log begin to extract
log_progress("Extract phase Started")
extracted_data = extract()

# Log complete for ectract
log_progress("Extract Phase ended")

# Log to begin Transform
log_progress("Transform Phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log Transform complete
log_progress('Transform phase ended')

# Log loading process started
log_progress("Load phase started")
load_data(target_file,transformed_data)

# Log completion of loading
log_progress("Load phase ended")

# Log the completion of ETL
log_progress("ETL Job Ended")

# end
