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
import requests

import s3_interface
import constants

logger = logging.getLogger()
logging.basicConfig(level=logging.WARNING)
logger.setLevel(logging.INFO)

def download_data(url):
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
        logger.error('Failed to retriev successfully retrieved from %s', url)
        # Return None
        return None

def get_dim(endpoint):
    """
    Get the data from the API and upload to S3
    
    Parameters:
        endpoint (str): The endpoint to get the data from
        file_name (str): The name of the file to save
    """
    # Get the data
    data = download_data(f"{constants.BASE_URL}/{endpoint}")

    # Upload file to s3
    s3_interface.upload_file(data, constants.S3_BUCKET, f'{endpoint}.json')

    return data

def lambda_handler(event, context):
    """
    Main method called by Lambda
    """
    logger.info("Starting data loading")
    startTime = float(time.time())

    get_dim(constants.COUNTRIES)
    get_dim(constants.TYPES)

    years = get_dim(constants.YEARS)

    # Retrieve data - even if requirement says since 2010, it's reasonable to get it all given the dataset size
    for year in years:
        current_url = f"{constants.BASE_URL}/data/all/{year['year']}"
        data = download_data(current_url)
        # Upload file to s3
        s3_interface.upload_file(data, constants.S3_BUCKET, f'data_{year["year"]}.json')

    logger.info("Finished in %s seconds", time.time() - startTime)

    return

if __name__ == '__main__':
    lambda_handler(None, None)
