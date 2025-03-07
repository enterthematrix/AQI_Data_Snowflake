import os

import requests
import sys
import logging

aqi_api_key = os.getenv("AQI_API_KEY")
# aqi_api_key = '579b464db66ec23bdd000001afc01b484e774fbc41c05c5ad1384070'
# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(levelname)s - %(message)s')


def get_air_quality_data(api_key, limit):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'

    # Parameters for the API request
    params = {
        'api-key': aqi_api_key,
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

            sf_session = snowpark_basic_auth()


            logging.info('Got the JSON Data')
            # Parse the JSON data from the response
            json_data = response.json()
            return json_data
        else:
            # Print an error message if the request was unsuccessful
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)
            # return None

    except Exception as e:
        # Handle exceptions, if any
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
        # return None
    # if comes to this line.. it will return nothing
    return None


limit_value = 4000
air_quality_data = get_air_quality_data(aqi_api_key, limit_value)
print(air_quality_data)