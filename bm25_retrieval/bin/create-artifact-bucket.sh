#!/bin/bash

set -e

MB_ARGS=$1

if ! [[ -f artifact-bucket.txt ]]; then
  BUCKET_ID=$(dd if=/dev/random bs=8 count=1 2>/dev/null | od -An -tx1 | tr -d ' \t\n')
  BUCKET_NAME=anlessini-lambda-artifacts-$BUCKET_ID
  aws s3 mb $1 s3://$BUCKET_NAME
  echo $BUCKET_NAME > artifact-bucket.txt
else
  aws s3 mb $1 s3://$(cat artifact-bucket.txt 2>/dev/null)
fi

