# Early Exiting MonoBERT

## Requirements

- Python 3.7+
- AWS CLI
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)

## Setup

Follow the guidline in [Early Exiting MonoBERT](https://github.com/castorini/earlyexiting-monobert)

## Move to Serverless

After training the model, copy the model over.

```bash
mkdir -p bert-base/msmarco/all-100/
pushd bert-base/msmarco/all-100/
cp -r your-path-to-earlyexiting-monobert/saved_models/bert-base/msmarco/all-42/ .
rm -rf epoch-0 epoch-1 epoch-2 # we only care about epoch-3
popd
```

Now let's provision the AWS infrastructure for BERT.

```bash
# build the BERT application
sam build

# create a new ECR repository
aws ecr create-repository --repository-name rerank --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE

# copy the value of "repositoryUri"

# deploy the application with a guided interactive mode
sam deploy -g
# name: rerank, choose the correct region, paste "repositoryUri" value and answer yes
```

To trigger BERT reranking, go to rerank Lambda in AWS console, click on API Gateway icon, copy API endpoint value and run

```bash
touch rerank-url && echo "your-api-endpoint-value" >> rerank-url

python3 test-rerank.py
```

And the output should be like `[['7187155', '0.9951216578483582']]`
