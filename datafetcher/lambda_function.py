"""
Fetch data from the Global Footprint Network API and upload to S3

Methods:
    get_json: Get the JSON data from the API
    upload_data_s3: Upload data to S3
    main: Main method
"""
#!/usr/bin/env python
import threading
import queue
import os
import time
import logging
from os import listdir
from os.path import isfile, join
import requests


import s3_interface
import constants

logger = logging.getLogger()
logging.basicConfig(level=logging.WARNING)
logger.setLevel(logging.INFO)

results = []
failed_requests = []
Ids = set()
num_workers = 3


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
    if response.ok:
        # Return the data
        logger.info('Data successfully retrieved from %s', url)
        return response.json()
    else:
        logger.error('Failed to retrieve from %s', url)
        # Return None
        return None

def worker(worker_num:int, q:queue) -> None:
    """Worker thread to get the data from the API

    Parameters:
        worker_num (int): The number of workers
        q (queue): The queue to get the info from
    """
    with requests.Session() as session:
        while True:
            Ids.add(f'Worker: {worker_num}, PID: {os.getpid()}, TID: {threading.get_ident()}')
            year = q.get()
            endpoint = f"data/all/{year}"
            print(f'WORKER {worker_num}: API request for year: {year} started ...')
            headers = {"HTTP_ACCEPT":"application/json"}

            response = session.get(url=constants.BASE_URL + endpoint, auth=(constants.GFN_USERNAME, constants.GFN_API_KEY), headers=headers, timeout=10)
            if response.ok:
                results.append(response.json())
            else:
                failed_requests.append(year)
            q.task_done()

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
    data = get_data(f"{constants.BASE_URL}{endpoint}")
    if write:
        write_to_local(data, f"{endpoint}.json")

    return data

def get_yearly_data(years):
    """
    Get the data for each year and save locally
    
    Parameters:
        years (list): The years to get the data for
    """
    # Create queue and add items
    q = queue.Queue()
    for year in years:
        q.put(year['year'])

    # turn-on the worker thread(s)
    # daemon: runs without blocking the main program from exiting
    for i in range(num_workers):
        threading.Thread(target=worker, args=(i, q), daemon=True).start()

    # block until all tasks are done
    q.join()

    write_to_local(results, "data.json")

    return

def lambda_handler(event, context):
    """
    Main method called by Lambda
    """
    logger.info("Starting data loading")
    start_time = float(time.time())

    download_data(constants.COUNTRIES)
    download_data(constants.TYPES)

    years = download_data(constants.YEARS)

    get_yearly_data(years)

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
