"""
Lambda function to transform data from the Global Footprint Network API and upload to S3
"""
import json
import urllib.parse
import logging
from json import JSONDecodeError
import boto3
import pandas as pd

s3_client = boto3.client('s3')
DESTINATION_BUCKET = "gfn-transformed"

logger = logging.getLogger()
logging.basicConfig(level=logging.WARNING)
logger.setLevel(logging.INFO)

class GfnEtl(object):

    """
    Transform data from the Global Footprint Network API and upload to S3
    """
    def cleanup_data(self, data_frame):
        """
        Clean up the data
        
        Parameters:
            data_frame (DataFrame): The data to clean up
        """
        # Remove non-ASCII characters and trim whitespaces
        data_frame.replace({r'[^\x00-\x7F]+':""}, regex=True, inplace=True)

        try:
            data_frame = data_frame.apply(lambda x: x.str.strip())
        except AttributeError:
            logger.error("Could not strip data")

    def transform_data(self, bucket, file_key):
        """
        Transform the data from the API to csv for snowpipe

        Parameters:
            bucket (str): The bucket the file was uploaded to
            file_key (str): The key of the file uploaded
        """
        try:
            # Get the object from the event
            logger.info(f"Added file Key: {file_key}")
            response = s3_client.get_object(Bucket=bucket, Key=file_key)
            response_body = response['Body'].read()  # returns bytes since Python 3.6+

            try:
                json_file = json.loads(response_body.decode('utf-8'))
                data_frame = pd.DataFrame(json_file)

                # Clean up the data
                self.cleanup_data(data_frame)

                # Get the file name
                file_name = file_key.split(".")[0]

                # Save locally to CSV
                loc_file = f"/tmp/{file_name}.csv"
                data_frame.to_csv(loc_file, index=False)

                # Upload to S3
                s3_client.upload_file(loc_file, DESTINATION_BUCKET, f"{file_name}.csv")
            except JSONDecodeError as json_decode_error:
                logging.info(f"Could not load file {file_key} uploaded to bucket: {bucket}")

        except Exception as ex:
            logger.info(ex)
            logger.error(f'Error getting object {file_key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
            raise ex


def lambda_handler(event, context):
    """
    Main method called by Lambda
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    gfnetl = GfnEtl()
    gfnetl.transform_data(bucket, key)
