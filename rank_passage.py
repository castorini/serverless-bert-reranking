import subprocess
import globals
import boto3
import logging
import os.path

s3 = boto3.client('s3')
logger = logging.getLogger(__name__)

def rerank():
    k = 1

    pc = '1.0'
    nc = '0.9'
    subprocess.call(['./scripts/eval_ee.sh', 'bert', 'base', 'msmarco', 'all', '70', pc, nc])

    # maybe add passage content later?
    return sorted(globals.results, key=lambda x: x[2], reverse=True)[0:k]

def lambda_handler(event, context):
    # TODO: update the query here
    query1 = "188714\t1000052\tfoods and supplements to lower blood sugar\tWatch portion sizes"
    query2 = "188714\t1022490\tfoods and supplements to lower blood sugar\tCinnamon, people who have diabetes, is commonly used to reduce blood sugar      and cholesterol level in blood. Onions contain falconoid and high sulfur which if consumed two ounces daily by diabetics reduces blood su     gar significantly. Garlic is a beneficial herb is another of the foods that lower blood sugar."

    if not os.path.isfile("/tmp/model.bin"):
        s3.download_file('model', 'model', '/tmp/model.bin')
        s3.download_file('model', 'config', '/tmp/config.json')
        s3.download_file('model', 'vocab', '/tmp/vocab.txt')
    globals.init()
    globals.queries.append(query1)
    globals.queries.append(query2)

    rerank()
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": "TODO"
    }
    return response

