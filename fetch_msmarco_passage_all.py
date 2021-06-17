import os
import json
import logging
import base64
import threading
import concurrent.futures
import time
import boto3
from botocore.exceptions import ClientError

DYNAMODB_TABLE = "anlessini-finally"
DYNAMODB_CONCURRENCY = int(os.environ.get("DYNAMODB_CONCURRENCY", 8))
DYNAMODB_RETRY_EXCEPTIONS = [
    "ProvisionedThroughputExceededException", "ThrottlingException"]

logger = logging.getLogger(__name__)

lambda_client = boto3.client("lambda")
thread_local = threading.local()


def _get_dynamo_client():
    if not hasattr(thread_local, "dynamo_client"):
        thread_local.session = boto3.session.Session()
        thread_local.dynamo_client = thread_local.session.resource("dynamodb")
    return thread_local.dynamo_client


def _batch_get_documents(hits, table=DYNAMODB_TABLE, max_retries=3):
    retries = 0
    results = []
    docids = {hit["docid"] for hit in hits}
    unprocessed_keys = [{"id": docid} for docid in docids]
    while unprocessed_keys:
        try:
            batch_get_response = _get_dynamo_client().batch_get_item(
                RequestItems={table: {"Keys": unprocessed_keys}}
            )
            results.extend(batch_get_response.get(
                "Responses", {}).get(table, []))
            unprocessed_keys = batch_get_response.get(
                "UnprocessedKeys", {}).get(table, [])
        except ClientError as e:
            if retries >= max_retries or e.response["Error"]["Code"] not in DYNAMODB_RETRY_EXCEPTIONS:
                raise
            time.sleep(2 ** (retries - 1))
            retries += 1
    return results


def get_documents(hits):
    """
    Obtain the document contents from DynamoDB as dicts
    :param hits: list of dicts, {docid: str, score: float, doc: int}
    :return: list of dicts, whose structure depends on the DynamoDB table schema, e.g., {"id": ..., "contents": ...}
    """
    documents = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=DYNAMODB_CONCURRENCY) as executor:
        hit_batches = [hits[i:i+100] for i in range(0, len(hits), 100)]
        for results in executor.map(_batch_get_documents, hit_batches):
            documents.extend(results)
    return documents


# just an example
#
# h = [{'docid': '7187155', 'score': 12.034427, 'doc': 7353292}]
# d = get_documents(h)
# print(d)

# output

#[{'contents': 'Applies To: System Center Global Service Monitor. 1  Definitions. \
#Capitalized terms used, but not defined herein, shall have the meanings given them in \
#the Master Agreement and/or Agreement. 2  Applicability of Supplemental Terms. These \
#Supplemental Terms apply only to Customerâ\x80\x99s purchase and use of Online Services \
#and Services. Products other than Online Services remain subject to the terms of the \
#Master Agreement, the Agreement, and any terms referenced therein.', 'id': '471142'}]
