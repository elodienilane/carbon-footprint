"""
Module to interact with AWS S3
"""
import logging
import os
import json
import boto3
from botocore.exceptions import ClientError
import constants

s3_client = boto3.client("s3")

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    Parameters:
        file_name (str): File to upload
        bucket (str): Bucket to upload to
        object_name (str): S3 object name. If not specified then file_name is used

    Returns:
        bool: True if file was uploaded, else False
    """

    if constants.MODE != "PROD":
        # Save file locally
        return save_file(file_name, bucket, object_name)

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as client_error:
        logging.error(client_error)
        return False
    return True


def save_file(data, bucket, file_name):
    """Save the data to a file
    
    Parameters:
        data (dict): The data to save
        bucket (str): The bucket to save the file to
        file_name (str): The name of the file to save
    """
    # Create the folder if it doesn't exist
    if not os.path.exists(bucket):
        os.makedirs(bucket)

    # Save the file locally
    dest = os.path.join(bucket, file_name)
    try:
        with open(dest, 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile)
    except IOError as io_error:
        logging.error(io_error)
        return False
    return True
