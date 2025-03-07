import os

import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging


# aqi_api_key = os.getenv("AQI_API_KEY")
aqi_api_key = '579b464db66ec23bdd000001afc01b484e774fbc41c05c5ad1384070'
#account = os.getenv("SNOWFLAKE_ACCOUNT")
account='XGGIWOA-YH85860',
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
# region = "ap-southeast-2"
region = "AWS_AP_SOUTHEAST_2"
role = "SYSADMIN"
database = "AQI_DB"
schema = "AQI_SCHEMA"
warehouse = "ADHOC_WH"
host = f"{account}.snowflakecomputing.com"




# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')

# Create the file name
file_name = f'air_quality_data_{timestamp}.json'

today_string = current_time_ist.strftime('%Y_%m_%d')

# Following credential has to come using secret whie running in automated way
def snowpark_basic_auth() -> Session:
    connection_parameters = {
       "ACCOUNT":account,
       "region":region,
        "HOST":host,
        "USER":user,
        "PASSWORD":password,
        "ROLE":role,
        "DATABASE":database,
        "SCHEMA":schema,
        "WAREHOUSE":warehouse
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def get_air_quality_data(api_key, limit):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'

    # Parameters for the API request
    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }

    # Headers for the API request
    headers = {
        'accept': 'application/json'
    }

    try:
        # Make the GET request
        response = requests.get(api_url, params=params, headers=headers)

        logging.info('Got the response, check if 200 or not')
        # Check if the request was successful (status code 200)
        if response.status_code == 200:

            logging.info('Got the JSON Data')
            # Parse the JSON data from the response
            json_data = response.json()


            logging.info('Writing the JSON file into local location before it moved to snowflake stage')
            # Save the JSON data to a file
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)

            logging.info(f'File Written to local disk with name: {file_name}')
            # Print debug messages
            print(f"SNOWFLAKE_ACCOUNT: {bool(account)}")  # Should print True
            print(f"SNOWFLAKE_USER: {bool(user)}")  # Should print True
            print(f"AQI_API_KEY: {bool(aqi_api_key)}")  # Should print True
            
            stg_location = '@aqi_db.aqi_schema.aqi_raw_data_stg/India/'+today_string+'/'
            sf_session = snowpark_basic_auth()
            
            logging.info(f'Placing the file, the file name is {file_name} and stage location is {stg_location}')
            sf_session.file.put(file_name, stg_location)
            
            logging.info('JSON File placed successfully in stage location in snowflake')
            lst_query = f'list {stg_location}{file_name}.gz'
            
            logging.info(f'list query to fetch the stage file to check if they exist there or not = {lst_query}')
            result_lst = sf_session.sql(lst_query).collect()
            
            logging.info(f'File is placed in snowflake stage location= {result_lst}')
            logging.info('The job over successfully...')
            
            # Return the retrieved data
            return json_data

        else:
            # Print an error message if the request was unsuccessful
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)
            #return None

    except Exception as e:
        # Handle exceptions, if any
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
        #return None
    #if comes to this line.. it will return nothing
    return None


limit_value = 4000
air_quality_data = get_air_quality_data(aqi_api_key, limit_value)