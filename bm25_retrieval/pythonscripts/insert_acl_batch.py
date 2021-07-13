"""
Insert ACL collection to DynamoDB as raw documents
"""

import argparse
import logging
import concurrent.futures
import time
import threading
import boto3
import boto3.session
from pyserini.search import SimpleSearcher

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)


def get_multivalued_field(doc, field):
    return [field.stringValue() for field in doc.getFields(field)]


MAX_BATCH_SIZE = 25  # DynamoDB batch_write_item limits the batch size to 25
ITEMS = {
    "id": "single",
    "contents": "single",
    "title": "single",
    "abstract_html": "single",
    "authors": "multi",
    "year": "single",
    "url": "single",
    "venues": "multi",
    "sigs": "multi"
}


def document_to_item(document):
    item = {}
    for field, value_type in ITEMS.items():
        item[field] = [field.stringValue() for field in document.getFields(field)] if value_type == "multi" else document.get(field)
        item[field] = item[field] or "None"
    return item


def build_item_batches(searcher, batch_size):
    batches = []
    curr_batch = []
    for docid in range(searcher.num_docs):
        document = searcher.doc(docid).lucene_document()
        item = document_to_item(document)
        curr_batch.append(item)
        if len(curr_batch) >= batch_size:
            batches.append(curr_batch)
            curr_batch = []
    if curr_batch:  # append the last small batch
        batches.append(curr_batch)
    return batches


thread_local = threading.local()


def get_dynamo_client():
    if not hasattr(thread_local, "dynamo_client"):
        thread_local.session = boto3.session.Session()
        thread_local.dynamo_client = thread_local.session.resource("dynamodb")
    return thread_local.dynamo_client


def batch_write_dynamo(table, items, max_retries=3):
    retries = 0
    failed_ids = []
    unprocessed_requests = [{"PutRequest": {"Item": item}} for item in items]
    while True:
        batch_write_response = get_dynamo_client().batch_write_item(RequestItems={table: unprocessed_requests})
        unprocessed_requests = batch_write_response.get("UnprocessedItems", {}).get(table, [])
        failed_ids = [request["PutRequest"]["Item"]["id"] for request in unprocessed_requests]
        if unprocessed_requests and retries < max_retries:
            logger.warning("Batch write failed for items %s, retrying in %s seconds", failed_ids, 2**retries)
            time.sleep(2**retries)
            retries += 1
        else:
            break
    return failed_ids


def main():
    parser = argparse.ArgumentParser("ACL Anthology document DynamoDB bulk importer")
    parser.add_argument("--index", required=True, type=str, help="Path to ACL Anthology Lucene index")
    parser.add_argument("--table", default="ACL", type=str, help="Dynamo table to insert the raw ACL documents to")
    parser.add_argument("--batch-size", dest="batch", default=MAX_BATCH_SIZE, help="The size of batch insert to Dynamo")
    parser.add_argument("--threads", default=5, type=int, help="Number of threads for batch inserts")
    parser.add_argument("--report-interval", dest="report_interval", default=500, type=int, help="Output progress interval")
    args = parser.parse_args()

    # TODO: use https://github.com/castorini/pyserini/blob/master/docs/usage-collection.md once AclAnthology support is added
    searcher = SimpleSearcher(args.index)

    progress = 0
    next_report_threshold = args.report_interval
    batches = build_item_batches(searcher, args.batch)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.batch) as executor:
        futures = {executor.submit(batch_write_dynamo, args.table, batch): batch for batch in batches}
        for future in concurrent.futures.as_completed(futures):
            batch = futures[future]
            try:
                failed_docids = future.result()
                if failed_docids:
                    logger.error("Error writing batches %s" % failed_docids)
            except Exception:
                batch_ids = [item["id"] for item in batch]
                logger.exception("Error writing batches %s" % batch_ids)
            finally:
                progress += len(batch)
                if progress > next_report_threshold:
                    logger.info("Processed %s/%s records" % (progress, searcher.num_docs))
                    next_report_threshold += args.report_interval


if __name__ == '__main__':
    main()
