import os
import subprocess
import uuid
import boto3
import logging
# import fetch_msmarco_passage_all

pc = '1.0'
nc = '0.9'
s3 = boto3.client('s3')
logger = logging.getLogger(__name__)

def rerank(uniq_id):
    subprocess.call(['./scripts/eval_ee.sh', 'bert', 'base', 'msmarco', 'all', str(uniq_id), pc, nc])


def get_documents(docid):
    return [{"contents": "Watch portion sizes", "id": "1000052"}, {"contents": "Cinnamon, people who have diabetes, is commonly used to reduce blood sugar and cholesterol level in blood. Onions contain falconoid and high sulfur which if consumed two ounces daily by diabetics reduces blood sugar significantly. Garlic is a beneficial herb is another of the foods that lower blood sugar.", "id": "1022490"}]

def lambda_handler(event, context):
    # TODO: update the query here

    query = "foods and supplements to lower blood sugar"
    ids = [{'docid': '1000052', 'score': 9.461265, 'doc': 6024913}, {'docid': '1022490', 'score': 9.461265, 'doc': 6024913}]
    docs = get_documents(ids)
    # docs = fetch_msmarco_passage_all.get_documents(ids)
    map = {}
    uniq_id = uuid.uuid4().int & (1<<64)-1
    with open('data/msmarco/dev_partitions/partition' + str(uniq_id), 'w') as f:
        for item in docs:
            docid = item["id"]
            content = item["contents"]
            map[docid] = content
            # query id doesnt matter in this case
            line = "1\t" + docid + "\t" + query + "\t" + content + "\n"
            f.write(line)

    rerank(uniq_id)

    with_score = []
    score_file = 'evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition' + str(uniq_id) + '.score'
    with open(score_file) as fin:
        for line in fin:
            line_arr = line.split('\t')
            with_score.append([line_arr[1], line_arr[2], map[line_arr[1]]])
    # print(with_score)

    # remove the files
    os.remove('evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition' + str(uniq_id) + '.score')
    os.remove('evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition' + str(uniq_id) + '.npy')
    os.remove('data/msmarco/dev_partitions/partition' + str(uniq_id))
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": with_score[0][1]
    }
    return response

