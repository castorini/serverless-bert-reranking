import logging
import boto3
import sys
from botocore.exceptions import ClientError


bucket_name = 'model'
region = None
# Create bucket
try:
    if region is None:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)

    s3_client.upload_file('model.bin', 'model', 'model/model.bin')
    s3_client.upload_file('model.bin', 'model', 'model/config.json')
    s3_client.upload_file('model.bin', 'model', 'model/vocab.txt')
except ClientError as e:
    logging.error(e)
