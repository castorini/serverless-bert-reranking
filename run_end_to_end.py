import csv
import math
import requests
import json
import time
import fetch_msmarco_passage_all
import argparse
from concurrent.futures import ThreadPoolExecutor

# Reading the evaluation dataset
tsv_file = open("queries.dev.small.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")

# Replace with your API URLs
search_url = "https://7wl1vh4ftb.execute-api.us-east-2.amazonaws.com/Prod/search"
rerank_url = "https://zcjb9x6wx5.execute-api.us-east-2.amazonaws.com/Prod/rerank"

count = 1

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, help='limit', required=True)

args = parser.parse_args()

# Here we extract the query string from the row and pass it on to our API
# which returns a json object containing the relevant docids

# top k results
k = 100
size = 10
docs = []
limit = args.limit

def rerank(i):
    rerank_params = {"docs": docs[i * size : i * size + size], "query": row[1]}
    while True:
        try:
            r = requests.post(rerank_url, json=rerank_params)
            if not r:
                print("retry rerank", i)
            else:
                break
        except requests.exceptions.ConnectionError:
            print("retry rerank conn issue", i)
            continue
    # return i
    return r.json()

open("output.txt", 'w').close()

# Here we attempt to read every line from our source file and extract the query out of it
# We want to retrieve 1000 passages for each query, which can be reranked later and we pick
# 100 passages out of them.

for row in read_tsv:
    params_args = {
        "query": row[1],
        "max_docs": 1000
    }

    # try until it works
    while True:
        r = requests.get(url=search_url, params=params_args)
        if not r:
            print("retry search")
        else:
            break

    r = r.json()

    # Calls the relevant functions to fetch passages from the DynamoDB table
    docs = fetch_msmarco_passage_all.get_documents(r)

    partitions = math.ceil(len(docs) / size)

    with ThreadPoolExecutor(max_workers=partitions) as pool:
        a = list(pool.map(rerank, range(0, partitions)))

    flat = [item for sublist in a for item in sublist]

    # Out of the 1000 passages, we only need top k passages that match our query
    result = sorted(flat, key=lambda x: float(x[1]), reverse=True)[0:k]
    
    # We store the file in this output format so that we can use trec_eval to measure the quality of our output
    with open("output.txt", "a") as f:
        for idx, item in enumerate(result):
            rank = idx + 1
            line =  row[0] + " " + "Q0 " + item[0] + " " + str(rank) + " " + str(item[1]) + " TEAM\n"
            f.write(line)
    print(str(count) + ' / ' + str(limit) + ' done')

    # Introducing limit for testing
    if count == limit:
        break

    count += 1
