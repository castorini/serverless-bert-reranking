# Serverless BM25 and BERT reranking pipeline

This repository contains the code for the paper titled "Serverless BM25  Search and BERT Reranking". As mentioned in the paper, the project was developed in two parts: BM25 retrieval and BERT reranking.

**Stage 1**: For BM25 retrieval task, refer to [bm25_retrieval](https://github.com/Ji-Xin/serverless-bert-reranking/tree/bm25/bm25_retrieval#bm25-retrieval-using-anlessini)

**Stage 2**: For BERT, the instructions can be found in [rerank](https://github.com/Ji-Xin/serverless-bert-reranking/tree/bm25/rerank#early-exiting-monobert)

This project is essentially a retrieval - reranking pipeline built on top of AWS. Once the instructions to set up BM25 retrieval and BERT are completed, the AWS API Gateway will provide a REST API endpoint using which the queries can be passed on to each stage.

We provide a script to run this pipeline end-to-end (`run_end_to_end.py`), where we have to specify the API URLs as well as path to our dataset where we need to read the queries from. For our experiment for the paper, we used the `dev` set from the MS MARCO passage dataset which can be found at `collections/msmarco-passage/queries.dev.small.tsv` once you set up [Anserini](https://github.com/castorini/anserini/blob/master/docs/experiments-msmarco-passage.md#retrieval) as part of setting up BM25 retrieval.

The format of the file (to be used as input) is something like below:
```bash
head collections/msmarco-passage/queries.dev.small.tsv
1048585	what is paula deen's brother
2	 Androgen receptor define
524332	treating tension headaches without medication
1048642	what is paranoid sc
524447	treatment of varicose veins in legs
786674	what is prime rate in canada
1048876	who plays young dr mallard on ncis
1048917	what is operating system misconfiguration
786786	what is priority pass
524699	tricare service number
```

So essentially any file with a `qid` or query id and query, separated by a `\t` will work for our input.

In `run_end_to_end.py`, we go through each query in our source file and first call our search API to return 1000 docids. This result is used to fetch the relevant passages from DynamoDB (see file `fetch_msmarco_passage_all.py`), where we'd ingested our corpus as part of setting up BM25 search. 

Before running the experiment, replace the url in `run_end_to_end.py` with your own

For testing purposes, we can pass `--limit` parameter which specifies the number of queries the code will run through, for example:

```python
python3 run_end_to_end.py --limit 100
```

We run through `limit` number of queries, for each query we retrieve 1000 passages using BM25, pass the passage content as well as the query to the BERT API for reranking, sorting the result further based on the score and pick top k (default is 100).
