"""
Fetch data from GFN API and upload to S3
"""
import time
import logging
from os import listdir
from os.path import isfile, join
import json

import requests
import constants
import s3_interface

logger = logging.getLogger()
logging.basicConfig(level=logging.WARNING)
logger.setLevel(logging.INFO)

class DataFetcher(object):
    """
    Fetch data from the Global Footprint Network API and upload to S3

    Methods:
        get_json: Get the JSON data from the API
        upload_data_s3: Upload data to S3
        main: Main method
    """

    def fetch_url(self, url):
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
        if response.ok:
            # Return the data
            logger.info('Data successfully retrieved from %s', url)
            return response.json()
        else:
            logger.error('Failed to retrieve from %s', url)
            return None

    def write_to_local(self, data, file_name, loc=constants.LOCAL_FILE_SYS):
        """
        Write the data to a local file
        
        Parameters:
            data (dict): The data to write
            file_name (str): The name of the file to write to
            loc (str): The location to write to
        """
        path = loc + "/" + file_name
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        return file_name

    def download_data(self, endpoint, write=True):
        """
        Get the data from the API and save locally
        
        Parameters:
            endpoint (str): The endpoint to get the data from
            write (bool): Whether to write the data to a local file
        """
        data = self.fetch_url(f"{constants.BASE_URL}{endpoint}")
        if write:
            self.write_to_local(data, f"{endpoint}.json")

        return data

    def get_yearly_data(self, years):
        """
        Get the data for each year and save locally
        
        Parameters:
            years (list): The years to get the data for
        """
        results = []
        failed_requests = []
        # Create queue and add items
        for year in years:
            if 'year' in year and year['year'] >= constants.MIN_YEAR:
                endpoint = f"data/all/{year['year']}"
                logger.info(f'API request for year: {year["year"]} started ...')
                data = self.fetch_url(f"{constants.BASE_URL}{endpoint}")
                if data:
                    results += data
                else:
                    failed_requests.append(year)

        if results:
            self.write_to_local(results, "data.json")
        else:
            logger.error("No data retrieved")

    def get_data(self):
        """
        Fetches data from the Global Footprint Network API
        """
        logger.info("Starting data fetcher")

        self.download_data(constants.COUNTRIES)
        self.download_data(constants.TYPES)

        years = self.download_data(constants.YEARS)

        # Get the data for each year > MIN_YEAR as set in constants.py
        self.get_yearly_data(years)

        # Upload all files in the local file system to S3
        files = [f for f in listdir(constants.LOCAL_FILE_SYS) if isfile(join(constants.LOCAL_FILE_SYS, f))]
        for f in files:
            s3_interface.upload_file(f"{constants.LOCAL_FILE_SYS}/{f}", constants.S3_BUCKET)


def lambda_handler(event, context):
    """
    Main method called by Lambda
    """
    data_fetcher = DataFetcher()
    _start = time.time()
    data_fetcher.get_data()
    logger.info("Execution time: %s seconds", (time.time() - _start))
