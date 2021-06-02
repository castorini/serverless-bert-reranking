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

    # reference https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    s3_client.upload_file('model/model.bin', 'model', 'model')
    s3_client.upload_file('model/config.json', 'model', 'config')
    s3_client.upload_file('model/vocab.txt', 'model', 'vocab')
except ClientError as e:
    logging.error(e)
