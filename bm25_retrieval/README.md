# BM25 Retrieval using Anlessini

## Requirements

- [Anserini](https://github.com/castorini/anserini): an open-source information retrieval toolkit built on Lucene.
- Java 11+
- Python 3.7+
- AWS CLI
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)

**Important**: It is recommended that to use to replicate this part of the project, a new AWS account which has access to the free tier benefits, be used (to minimize costs)

## Get Started
   
First let's build the project.

```bash
$ mvn clean install
```

Note: If you receive an error regarding Docker TestContainers failing to load or using incorrect Docker version, run `docker system prune -af` to clean up all docker related content (images etc.) and run the above command again.

If you get any permission issues with AWS CLI, try to run the above command with sudo permissions.

Anlessini uses AWS SAM/Cloudformation for describing the infrastructure.
So let's create a S3 bucket for storing the artifacts. If you already have an existing artifacts bucket due to a prior run of this project, then no need to create a new one.

```bash
$ ./bin/create-artifact-bucket.sh
```

Now let's provision the AWS infrastructure for Anlessini.
We recommend that you spin up individual CloudFormation stack for each of the collection, as they are logically isolated.
In this task, we would be using the MS MARCO passage dataset [MS MARCO passage dataset](https://github.com/castorini/anserini/blob/master/docs/experiments-msmarco-passage.md).

```bash
# package the artifact and upload to S3
$ sam package --template-file template.yaml --s3-bucket $(cat artifact-bucket.txt) --output-template-file cloudformation/msmarco.yaml --s3-prefix msmarco
# create cloudformation stack
$ sam deploy --template-file cloudformation/msmarco.yaml $(cat artifact-bucket.txt) --s3-prefix msmarco --stack-name msmarco --capabilities CAPABILITY_NAMED_IAM
```

Now we have our infrastructure up, we can populate S3 with our index files, and import the corpus into DynamoDB.

We will be using [Anserini](https://github.com/castorini/anserini) to index our corpus, so please refer to the [documentation](https://github.com/castorini/anserini/tree/master/docs) for your specific corpus. 

First, download and extract the corpus.

```bash
$ cd /path/to/anserini
$ mkdir collections/msmarco-passage
$ wget https://www.dropbox.com/s/9f54jg2f71ray3b/collectionandqueries.tar.gz -P collections/msmarco-passage
$ tar xvfz collections/msmarco-passage/collectionandqueries.tar.gz -C collections/msmarco-passage
$ python tools/scripts/msmarco/convert_collection_to_jsonl.py \
 --collection-path collections/msmarco-passage/collection.tsv \
 --output-folder collections/msmarco-passage/collection_jsonl
```

Now we will build the Lucene index.
Note that we **do not enable** `-storeContents`, `-storeRaw`, or `-storePositions` to keep the index minimal. 
Keeping an index small helps speed up search queries.

```bash
$ cd /path/to/anserini
$ mvn clean package appassembler:assemble -e
$ sh target/appassembler/bin/IndexCollection -threads 9 -collection JsonCollection \
 -generator DefaultLuceneDocumentGenerator -input collections/msmarco-passage/collection_jsonl \
 -index indexes/msmarco-passage/lucene-index-msmarco -storeDocvectors
```
Upon completion, we should have an index with 8,841,823 documents

Now lets upload the index files to S3.

```bash
$ cd /path/to/anserini
$ export INDEX_BUCKET=$(aws cloudformation describe-stacks --stack-name msmarco --query "Stacks[0].Outputs[?OutputKey=='IndexBucketName'].OutputValue" --output text)
$ aws s3 cp indexes/msmarco-passage/lucene-index-msmarco/ s3://$INDEX_BUCKET/msmarco/ --recursive
```

A quick check here is to make sure the bucket exists on the console, and has a sub-directory `msmarco` and inside that you have all the index files.

To import the corpus into DynamoDB, use the `ImportCollection` util.
You may first run the command with `-dryrun` option to perform validation and sanity check without writing to DynamoDB. 
If everything goes well in the dryrun, you can write the document contents to DynamoDB.

With `Provisioned` Read/Write capacity it takes a bit longer (~ 1.5 - 2 hours) as compared to on-demand but costs negligible as it is under the free tier. A quick check here would be to check using the console that the `DYNAMO_TABLE` actually has its Read/Write capacity set to "provisioned" and not "on-demand", to avoid incurring unnecessary costs. We tested with the default fields that were assigned under the provisioned option.

```bash
$ cd /path/to/anlessini
$ export DYNAMO_TABLE=$(aws cloudformation describe-stacks --stack-name msmarco --query "Stacks[0].Outputs[?OutputKey=='DynamoTableName'].OutputValue" --output text)
$ utils/target/appassembler/bin/ImportCollection \
    -collection JsonCollection -generator DefaultLuceneDocumentGenerator \
    -dynamo.table $DYNAMO_TABLE \
    -threads 8 -input path/to/anserini/collections/msmarco-passage 
```

Now we can try invoking our function:

```bash
$ export API_URL=$(aws cloudformation describe-stacks --stack-name msmarco --query "Stacks[0].Outputs[?OutputKey=='SearchApiUrl'].OutputValue" --output text)
$ curl $API_URL\?query\=America\&max_docs\=3
```

For a complex sentence-based query try:
```bash
$ curl $API_URL\?query\=What%20is%20the%20capital%20of%20France%20?\&max_docs\=3 # replacing spaces with %20
```

A few initial `curl` requests may time out due to AWS API gateway's 30 second limit (https://github.com/serverless/serverless/issues/3171) but the consistency should pick up after a few requests.

