import csv
import math
import requests
import json
import time
import fetch_msmarco_passage_all
from concurrent.futures import ThreadPoolExecutor

# Reading the evaluation dataset
tsv_file = open("queries.dev.small.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")

search_url = "https://7wl1vh4ftb.execute-api.us-east-2.amazonaws.com/Prod/search/"
rerank_url = "https://zcjb9x6wx5.execute-api.us-east-2.amazonaws.com/Prod/rerank"

count = 1

# Here we extract the query string from the row and pass it on to our API
# which returns a json object containing the relevant docids

# top k results
k = 100
size = 10
ids = []
limit = 10

def rerank(i):
    rerank_params = {"ids": ids[i * size : i * size + size], "query": row[1]}
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

for row in read_tsv:
    params_args = {
        "query": row[1],
        "max_docs": 1000
    }

    # try until it works
    start = time.time()
    while True:
        r = requests.get(url=search_url, params=params_args)
        if not r:
            print("retry search")
        else:
            break

    ids = r.json()
    # docs = fetch_msmarco_passage_all.get_documents(r)
    partitions = math.ceil(len(ids) / size)
    with ThreadPoolExecutor(max_workers=partitions) as pool:
        a = list(pool.map(rerank, range(0, partitions)))

    flat = [item for sublist in a for item in sublist]

    result = sorted(flat, key=lambda x: float(x[1]), reverse=True)[0:k]
    with open("output.txt", "a") as f:
        for idx, item in enumerate(result):
            rank = idx + 1
            line =  row[0] + " " + "Q0 " + item[0] + " " + str(rank) + " " + str(item[1]) + " TEAM\n"
            f.write(line)
    end = time.time()
    print(end - start)
    print(str(count) + ' / ' + str(limit) + ' done')
    if count == limit:
        break
    count += 1
