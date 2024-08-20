#import libraries and setting paths

import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" #stores the final output that it load to a database
target_file = "transformed_data.csv" # stores all the logs

#Extraction

#extract csv's files

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

#extract json's files

def extract_from_json(file_to_process):
    dataframe= pd.read_json(file_to_process, lines=True)
    return dataframe

#extract xml's files

#first we need to parse the data from the file using ElementTree function.


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name","height", "weight"])
    tree= ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)

    return dataframe



#extract global, uses the glob library to identify the filetype.
def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data 
     
    # process all csv files 
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 



#Transformation

def transform(data):

    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data.height * 0.0254,2) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2) 

    return data




#Loading and logging

#load the transformed data to a CSV file 

#parameters: target file and transformed data 

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


 #logging 

''' Finally, you need to implement the logging operation to record the 
progress of the different operations.

For this operation, you need to
record a message, along with its timestamp, in the log_file.

To record the message, you need to implement a function log_progress() 
that accepts the log message as the argument. The function captures 
the current date and time using the datetime function from the datetime 
library. The use of this function requires the definition of a date-time 
format, and you need to convert the timestamp to a string format using the 
strftime attribute. The following code creates the log operation:'''

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 




# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

