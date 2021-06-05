import os
import subprocess
import uuid
import boto3
import logging

pc = '1.0'
nc = '0.9'
s3 = boto3.client('s3')
logger = logging.getLogger(__name__)

def rerank(uniq_id):
    k = 1

    subprocess.call(['./scripts/eval_ee.sh', 'bert', 'base', 'msmarco', 'all', str(uniq_id), pc, nc])

    # maybe add passage content later?
    # return sorted(globals.results, key=lambda x: x[2], reverse=True)[0:k]

def get_content(docid):
    content1 = "Watch portion sizes"
    content2 = "Cinnamon, people who have diabetes, is commonly used to reduce blood sugar      and cholesterol level in blood. Onions contain falconoid and high sulfur which if consumed two ounces daily by diabetics reduces blood su     gar significantly. Garlic is a beneficial herb is another of the foods that lower blood sugar."
    map = {"1000052": content1, "1022490": content2}

    # parser = argparse.ArgumentParser()
    # parser.add_argument("--id", type=str, help='id', required=True)
    # args = parser.parse_args()
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    # msmarco = dynamodb.Table('anlessini-finally') # replace with the right table name
    # response = msmarco.get_item(Key={"id": args.id})
    # print(response["Item"]["contents"])
    return map[docid]

def lambda_handler(event, context):
    # TODO: update the query here
    # query1 = "188714\t1000052\tfoods and supplements to lower blood sugar\tWatch portion sizes\n"
    # query2 = "188714\t1022490\tfoods and supplements to lower blood sugar\tCinnamon, people who have diabetes, is commonly used to reduce blood sugar      and cholesterol level in blood. Onions contain falconoid and high sulfur which if consumed two ounces daily by diabetics reduces blood su     gar significantly. Garlic is a beneficial herb is another of the foods that lower blood sugar.\n"

    query = "foods and supplements to lower blood sugar"
    docs = [{'docid': '1000052', 'score': 9.461265, 'doc': 6024913}, {'docid': '1022490', 'score': 9.461265, 'doc': 6024913}]
    map = {}
    uniq_id = uuid.uuid4().int & (1<<64)-1
    with open('data/msmarco/dev_partitions/partition' + str(uniq_id), 'w') as f:
        for item in docs:
            docid = item["docid"]
            content = get_content(docid)
            map[docid] = content
            # query id doesnt matter in this case
            line = "1\t" + docid + "\t" + query + "\t" + content + "\n"
            f.write(line)
    # if not os.path.isfile("/tmp/model.bin"):
    #     s3.download_file('model', 'model', '/tmp/model.bin')
    #     s3.download_file('model', 'config', '/tmp/config.json')
    #     s3.download_file('model', 'vocab', '/tmp/vocab.txt')

    rerank(uniq_id)
    with_score = []
    score_file = 'evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition' + str(uniq_id) + '.score'
    with open(score_file) as fin:
        for line in fin:
            line_arr = line.split('\t')
            with_score.append([line_arr[1], line_arr[2]])
    print(with_score)

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

