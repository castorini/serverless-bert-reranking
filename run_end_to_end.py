import csv
import requests
import json
import time

# Reading the evaluation dataset
tsv_file = open("queries.eval.small.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")

api_url = "https://7wl1vh4ftb.execute-api.us-east-2.amazonaws.com/Prod/search/"

count = 1

# Here we extract the query string from the row and pass it on to our API
# which returns a json object containing the relevant docids

for row in read_tsv:
    time.sleep(0.5)
    params_args = {
        "query": row[1],
        "max_docs": 1000
    }
    r = requests.get(url=api_url, params=params_args)
    print(count, r.json())
    count += 1
