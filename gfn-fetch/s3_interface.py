"""
Module to interact with AWS S3
"""
import logging
import os
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
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    if constants.MODE != "PROD":
        # Save file locally
        return True


    # Upload the file
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as client_error:
        logging.error(client_error)
        return False
    return True
