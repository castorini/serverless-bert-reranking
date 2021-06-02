import os
import json
import logging
import base64
import threading
import concurrent.futures
import time
import boto3
from botocore.exceptions import ClientError

SEARCH_LAMBDA_ARN = os.environ["SEARCH_LAMBDA"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
DYNAMODB_CONCURRENCY = int(os.environ.get("DYNAMODB_CONCURRENCY", 8))
DYNAMODB_RETRY_EXCEPTIONS = ["ProvisionedThroughputExceededException", "ThrottlingException"]

logger = logging.getLogger(__name__)

lambda_client = boto3.client("lambda")
thread_local = threading.local()


def invoke_search_lambda(query, max_docs):
    """
    Query the Lucene lambda to get a ordered list of hits
    :param query: query string
    :param max_docs: max number of documents
    :return: list of hits, each hit being a dict of {docid: str, score: float, doc: int}
    """
    invocation_params = {
        "query": query,
        "maxDocs": max_docs
    }
    invocation = lambda_client.invoke(
        FunctionName=SEARCH_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(invocation_params).encode("utf-8")
    )
    invocation_payload = invocation["Payload"].read().decode("utf-8")
    if "FunctionError" in invocation or invocation["StatusCode"] != 200:
        log_message = base64.b64decode(invocation["LogResult"]) if invocation["LogResult"] else ""
        raise Exception("Search invocation failed", invocation["FunctionError"], log_message, invocation_payload)

    payload_json = json.loads(invocation_payload)
    hits = payload_json["hits"]
    return hits


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
            results.extend(batch_get_response.get("Responses", {}).get(table, []))
            unprocessed_keys = batch_get_response.get("UnprocessedKeys", {}).get(table, [])
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


def lambda_handler(event, context):
    query = event["queryStringParameters"]["query"]
    max_docs = event["queryStringParameters"].get("max_docs")

    hits = invoke_search_lambda(query, max_docs)
    documents = get_documents(hits)

    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(documents)
    }
    return response
