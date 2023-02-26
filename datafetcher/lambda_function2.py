import time
from multiprocessing import Process, Pipe
# import boto3
import logging
import os
from os import listdir
from os.path import isfile, join

import threading
import queue
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
    results = []
    failed_requests = []
    Ids = set()
    num_workers = 3

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
            # Return None
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
            file.write(str(data))
        return file_name

    def download_data(self, endpoint, write=True):
        """
        Get the data from the API and save locally
        
        Parameters:
            endpoint (str): The endpoint to get the data from
            write_to_local (bool): Whether to write the data to a local file
        """
        data = self.fetch_url(f"{constants.BASE_URL}{endpoint}")
        if write:
            self.write_to_local(data, f"{endpoint}.json")

        return data

    def worker(self, worker_num:int, q:queue) -> None:
        """Worker thread to get the data from the API

        Parameters:
            worker_num (int): The number of workers
            q (queue): The queue to get the info from
        """
        with requests.Session() as session:
            while True:
                self.Ids.add(f'Worker: {worker_num}, PID: {os.getpid()}, TID: {threading.get_ident()}')
                year = q.get()
                endpoint = f"data/all/{year}"
                logger.info(f'WORKER {worker_num}: API request for year: {year} started ...')
                headers = {"HTTP_ACCEPT":"application/json"}

                url = constants.BASE_URL + endpoint
                response = session.get(url=url, auth=(constants.GFN_USERNAME, constants.GFN_API_KEY), headers=headers, timeout=10)
                if response.ok:
                    logger.info('Data successfully retrieved from %s', url)
                    self.results.append(response.json())
                else:
                    logger.error('Failed to retrieve from %s', url)
                    self.failed_requests.append(year)
                q.task_done()

    def get_yearly_data(self, years):
        """
        Get the data for each year and save locally
        
        Parameters:
            years (list): The years to get the data for
        """
        # Create queue and add items
        q = queue.Queue()
        for year in years:
            if 'year' in year and year['year'] >= constants.MIN_YEAR:
                q.put(year['year'])

        # turn-on the worker thread(s)
        # daemon: runs without blocking the main program from exiting
        for i in range(self.num_workers):
            threading.Thread(target=self.worker, args=(i, q), daemon=True).start()

        # block until all tasks are done
        q.join()

        self.write_to_local(self.results, "data.json")

    def get_data(self):
        """
        Fetches data from the Global Footprint Network API
        """
        logger.info("Starting data fetcher")

        self.download_data(constants.COUNTRIES)
        self.download_data(constants.TYPES)

        years = self.download_data(constants.YEARS)

        self.get_yearly_data(years)

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

if __name__ == '__main__':
    """
    Used for local testing
    """
    lambda_handler(None, None)
