"""
Fetch data from the Global Footprint Network API and upload to S3

Methods:
    get_json: Get the JSON data from the API
    upload_data_s3: Upload data to S3
    main: Main method
"""
#!/usr/bin/env python
import time
import logging
from os import listdir
from os.path import isfile, join
import json
import requests


import s3_interface
import constants

logger = logging.getLogger()
logging.basicConfig(level=logging.WARNING)
logger.setLevel(logging.INFO)


def get_data(url):
    """Get the data from the API
    
    Parameters:
        url (str): The URL to get the data from

    Returns:
        dict: The data from the API, None if failed
    """
    # Get the data from the API
    headers = {"HTTP_ACCEPT":"application/json"}

    response = requests.get(url, auth=(constants.GFN_USERNAME, constants.GFN_API_KEY), headers=headers, timeout=10)

    # Check the status code
    if response.status_code == 200:
        # Return the data
        logger.info('Data successfully retrieved from %s', url)
        return response.json()
    else:
        logger.error('Failed to retrieve from %s', url)
        # Return None
        return None

def write_to_local(data, file_name, loc=constants.LOCAL_FILE_SYS):
    """
    Write the data to a local file
    
    Parameters:
        data (dict): The data to write
        file_name (str): The name of the file to write to
        loc (str): The location to write to
    """
    path = loc + "/" + file_name
    with open(path, "w", encoding="utf-8") as file:
        file.write(str(data))
    return file_name

def download_data(endpoint, write=True):
    """
    Get the data from the API and save locally
    
    Parameters:
        endpoint (str): The endpoint to get the data from
        write_to_local (bool): Whether to write the data to a local file
    """
    data = get_data(f"{constants.BASE_URL}/{endpoint}")
    if write:
        write_to_local(data, f"{endpoint}.json")

    # # Upload file to s3
    # s3_interface.upload_file(data=data, bucket=constants.S3_BUCKET, object_name=f'{endpoint}.json', mode=constants.MODE)

    return data

def lambda_handler(event, context):
    """
    Main method called by Lambda
    """
    logger.info("Starting data loading")
    start_time = float(time.time())

    download_data(constants.COUNTRIES)
    download_data(constants.TYPES)

    years = download_data(constants.YEARS)

    full_data = []
    # Retrieve data - even if requirement says since 2010, it's reasonable to get it all given the dataset size
    for year in years:
        current_url = f"data/all/{year['year']}"
        data = download_data(current_url, write=False)

        full_data.append(data)
        # Upload file to s3
        # s3_interface.upload_file(data=data, bucket=constants.S3_BUCKET, object_name=f'data_{year["year"]}.json')
    
    write_to_local(full_data, "data.json")

    files = [f for f in listdir(constants.LOCAL_FILE_SYS) if isfile(join(constants.LOCAL_FILE_SYS, f))]
    for f in files:
        s3_interface.upload_file(f"{constants.LOCAL_FILE_SYS}/{f}", constants.S3_BUCKET)

    logger.info("Finished in %s seconds", time.time() - start_time)

    return

if __name__ == '__main__':
    """
    Used for local testing
    """
    lambda_handler(None, None)
