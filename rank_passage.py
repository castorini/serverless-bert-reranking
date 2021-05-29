import subprocess
from pathlib import Path
Path("data/msmarco/dev_partitions").mkdir(parents=True, exist_ok=True)

# the query should look like this
# 188714  1000052 foods and supplements to lower blood sugar      Watch portion sizes
query = "188714\t1000052\tfoods and supplements to lower blood sugar\tWatch portion sizes"
k = 4
qid = query.split()[0]

filtered_result = []
map = {}
with open("data/msmarco/top1000.dev") as fin:
    for line in fin:
        if line.startswith(qid):
            line_arr = line.split('\t')
            map[line_arr[1]] = line_arr[3]
            filtered_result.append(line)

with open('data/msmarco/dev_partitions/partition70', 'w') as f:
    for item in filtered_result:
        f.write(item)

pc = '1.0'
nc = '0.9'
# subprocess.call(['./scripts/eval_ee.sh', 'bert', 'base', 'msmarco', 'all', '70', pc, nc])

with_score = []
score_file = 'evaluation/msmarco/pc-' + pc + '-nc-' + nc + '/dev.partition70.score'
with open(score_file) as fin:
    for line in fin:
        line_arr = line.split('\t')
        with_score.append([line_arr[1], map[line_arr[1]], line_arr[2]])

print(sorted(with_score, key=lambda x: x[2], reverse=True)[0:k])
