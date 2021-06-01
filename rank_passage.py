import subprocess
import boto3
import logging
import os.path
from pathlib import Path

s3 = boto3.client('s3')
logger = logging.getLogger(__name__)
Path("/tmp/data/msmarco/dev_partitions").mkdir(parents=True, exist_ok=True)

def rerank(query):
    # the query should look like this
    # 188714  1000052 foods and supplements to lower blood sugar      Watch portion sizes
    k = 4
    qid = query.split()[0]

    s3.download_file('collections', qid, '/tmp/data/msmarco/dev_partitions/partition70')

    pc = '1.0'
    nc = '0.9'
    subprocess.call(['./scripts/eval_ee.sh', 'bert', 'base', 'msmarco', 'all', '70', pc, nc])

    with_score = []
    score_file = 'evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition70.score'
    # maybe add passage content later?
    with open(score_file) as fin:
        for line in fin:
            line_arr = line.split('\t')
            with_score.append([line_arr[1], line_arr[2]])

    return sorted(with_score, key=lambda x: x[2], reverse=True)[0:k]

def lambda_handler(event, context):
    # TODO: update the query here
    query = "188714\t1000052\tfoods and supplements to lower blood sugar\tWatch portion sizes"
    if not os.path.isfile("/tmp/model.bin"):
        s3.download_file('model', 'model', '/tmp/model.bin')
        s3.download_file('model', 'config', '/tmp/config.json')
        s3.download_file('model', 'vocab', '/tmp/vocab.txt')

    rerank(query)
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": "TODO"
    }
    return response

