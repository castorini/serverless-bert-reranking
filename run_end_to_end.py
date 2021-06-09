import csv
import requests
import json
import time
import fetch_msmarco_passage_all

# Reading the evaluation dataset
tsv_file = open("queries.eval.small.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")

search_url = "https://7wl1vh4ftb.execute-api.us-east-2.amazonaws.com/Prod/search/"
rerank_url = "https://zcjb9x6wx5.execute-api.us-east-2.amazonaws.com/Prod/rerank"

count = 1

# Here we extract the query string from the row and pass it on to our API
# which returns a json object containing the relevant docids

# top k result
k = 3

for row in read_tsv:
    time.sleep(0.5)
    params_args = {
        "query": row[1],
        "max_docs": 1000
    }
    r = requests.get(url=search_url, params=params_args).json()
    docs = fetch_msmarco_passage_all.get_documents(r)
    rerank_params = {"docs": docs, "query": row[1]}
    # TODO: split to 100 req here
    # 1000/100 will timeout
    r = requests.post(rerank_url, json=rerank_params).json()
    print(sorted(r, key=lambda x: x[1], reverse=True)[0:k])
    
    break
    count += 1
