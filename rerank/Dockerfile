FROM public.ecr.aws/lambda/python:3.7

RUN pip3 install torch==1.3.1+cpu torchvision==0.4.2+cpu -f https://download.pytorch.org/whl/torch_stable.html

RUN pip3 install tqdm tensorboardX boto3 regex sentencepiece sacremoses scikit-learn requests

COPY data/ /var/task/data
COPY evaluation/ /var/task/evaluation
COPY examples/ /var/task/examples
COPY rerank.py /var/task/
COPY logs/ /var/task/logs/
COPY saved_models/ /var/task/saved_models/
COPY scripts/ /var/task/scripts
COPY transformers/ /var/task/transformers

RUN mkdir -p /tmp/dev_partitions

# Command can be overwritten by providing a different command in the template directly.
CMD ["rerank.lambda_handler"] 
