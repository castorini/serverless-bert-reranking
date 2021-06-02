import argparse
import boto3
import json
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=str, help='id', required=True)
    args = parser.parse_args()
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    msmarco = dynamodb.Table('anlessini-finally') # replace with the right table name
    response = msmarco.get_item(Key={"id": args.id})
    print(response["Item"]["contents"])

# For example: python fetch.py --id 8094497
# Output: Blue Microphones. A Yeti USB microphone. Blue Microphones is an audio production company that designs and 
# produces microphones, headphones, recording tools, signal processors, and music acc essories for audio professionals, 
# musicians and consumers.
