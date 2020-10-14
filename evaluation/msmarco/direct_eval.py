import os
import argparse
from collections import defaultdict
import numpy as np
from msmarco_eval import load_reference, compute_metrics_from_files

MaxMRRRank = 10


def dd_list_wrapper():
    return defaultdict(list)


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--split", required=False, default="dev", type=str,
                        help="Which split to evaluate (dev or eval)")
    parser.add_argument("-p", "--partition", required=False, default="all", type=str,
                        help="Which partitions to evaluate ('all', or comma separated list")
    parser.add_argument("-r", "--reference", required=False, default="qrels.dev.small.tsv",
                        help="Qrel file")
    parser.add_argument("-l", "--n_layers", required=False, default=12, type=int,
                        help="Number of layers")
    parser.add_argument("--np_file", required=False, default='results.npy', type=str,
                        help="File name to save results in")
    parser.add_argument("--sp_folder", required=False, default=None, type=str,
                        help="Evaluate a specific folder")

    parser.add_argument("-pc", "--positive_confidence", required=False, default='1.0', type=str,
                        help="HP positive confidence: exit when positive confidence > this value")
    parser.add_argument("-nc", "--negative_confidence", required=False, default='1.0', type=str,
                        help="HP negative confidence: exit when negative confidence > this value")

    parser.add_argument("--no_save_np", action='store_true',
                        help="Don't save to np files")
    parser.add_argument("--detail", action='store_true',
                        help="Print out scores in each layer")
    parser.add_argument("--each_layer", action='store_true',
                        help="Evaluate each layer's result (override others)")

    args = parser.parse_args()

    all_partition = 70 if args.split == 'dev' else 69
    if args.partition == 'all':
        args.partition = list(range(all_partition))
        args.full_partition = True
    else:
        args.partition = args.partition.split(',')
        args.full_partition = False

    args.positive_confidence = float(args.positive_confidence)
    args.negative_confidence = float(args.negative_confidence)

    return args


def load_collection(args):
    collection = defaultdict(dd_list_wrapper)
    # key: qid; value: dict [key: pid, value: listof score (for each layer)]
    for layer in range(args.n_layers):
        for par in args.partition:
            with open(os.path.join(f'layer{layer}', f'{args.split}.partition{par}.score')) as fin:
                for line in fin:
                    qid, pid, score = line.strip().split('\t')
                    collection[qid][pid].append(float(score))
    return collection


def print_relevant_pair_details(args, collection):
    reference = load_reference(args.reference)
    with open('rel_pair.txt', 'w') as fout:
        for qid, rel_list in reference.items():
            for pid in rel_list:
                if str(qid) not in collection or str(pid) not in collection[str(qid)]:
                    continue  # prevent new keys being added to the defaultdict
                print(f'{qid} {pid}', end='\t', file=fout)
                for j, sc in enumerate(collection[str(qid)][str(pid)]):
                    print('{:.1f}'.format(100*sc), end=' ', file=fout)
                print(file=fout)


def generate_final_scores_confidence(args, collection):
    final_scores = defaultdict(list)  # key: qid; value: list of [pid, score]
    used_layers = 0
    pairs = 0
    for qid, qid_value in collection.items():
        for pid, score_list in qid_value.items():
            pairs += 1
            for i in range(args.n_layers):
                if (i+1 == args.n_layers or
                        score_list[i] > args.positive_confidence or
                        score_list[i] < 1-args.negative_confidence
                ):
                    final_scores[qid].append([pid, score_list[i], i+1, score_list])
                    used_layers += i+1
                    break

    return final_scores, used_layers / pairs


def generate_run_file(args, final_scores):
    run_fname = args.split + '.run'
    with open(run_fname, 'w') as fout:
        for qid, qid_value in final_scores.items():
            qid_value.sort(key=lambda x: x[1], reverse=True)  # sort by score, max on top
            for i in range(MaxMRRRank):
                if i == len(qid_value):
                    break
                if not args.detail:
                    print('{}\t{}\t{}\t{}\t{}'.format(
                        qid, qid_value[i][0], i+1, qid_value[i][1], qid_value[i][2]
                    ), file=fout)
                else:
                    print('{}\t{}\t{}\t{:.1f} {}'.format(
                        qid, qid_value[i][0], i + 1, qid_value[i][1]*100, qid_value[i][2]
                    ), end='\t', file=fout)
                    for j, sc in enumerate(qid_value[i][3]):
                        print('{:.1f}'.format(100*sc), end=('\t' if 2*(j+1)==args.n_layers else ' '), file=fout)
                    print(file=fout)
    return run_fname


def eval_each_layer(args, collection):
    with open('each_layer.txt', 'w') as fout:
        for i in range(args.n_layers):
            final_scores = defaultdict(list)  # key: qid; value: list of [pid, score]
            for qid, qid_value in collection.items():
                for pid, score_list in qid_value.items():
                    final_scores[qid].append([pid, score_list[i], i+1, score_list])

            run_fname = generate_run_file(args, final_scores)
            metrics = compute_metrics_from_files(args.reference, run_fname)
            print(f'Layer {i}:\t{metrics["MRR @10"]}', file=fout)


def eval_specific_folder(args):
    final_scores = defaultdict(list)  # key: qid; value: list of [pid, score]
    used_layers = 0
    pairs = 0

    for par in args.partition:
        with open(os.path.join(
                args.sp_folder,
                f'{args.split}.partition{par}.score'
        )) as fin:
            for line in fin:
                qid, pid, score, layer = line.strip().split('\t')
                pairs += 1
                final_scores[qid].append([pid, float(score), int(layer), []])
                used_layers += int(layer)

    run_fname = generate_run_file(args, final_scores)
    print('Avg-layer:', used_layers / pairs)
    metrics = compute_metrics_from_files(args.reference, run_fname)
    for metric in sorted(metrics):
        print('{}: {}'.format(metric, metrics[metric]))


def save_to_np(args, metrics, avg_layer):
    if os.path.exists(args.np_file):
        record = np.load(args.np_file, allow_pickle=True).item()
    else:
        record = {}

    key = (args.positive_confidence, args.negative_confidence)
    value = (avg_layer, metrics["MRR @10"])
    record[key] = value
    np.save(args.np_file, np.array(record))


def main():
    args = get_args()

    if args.sp_folder is not None:
        eval_specific_folder(args)
        exit(0)

    if args.full_partition:
        cache_fname = 'full_collection.npy'
        if os.path.exists(cache_fname):
            collection = np.load(cache_fname, allow_pickle=True).item()
        else:
            collection = load_collection(args)
            np.save(cache_fname, collection)
    else:
        collection = load_collection(args)

    if args.each_layer:
        eval_each_layer(args, collection)
        exit(0)
    if args.detail:
        print_relevant_pair_details(args, collection)
    final_scores, avg_layer = generate_final_scores_confidence(args, collection)
    run_fname = generate_run_file(args, final_scores)
    metrics = compute_metrics_from_files(args.reference, run_fname)
    print('Avg layer: {}'.format(avg_layer))
    for metric in sorted(metrics):
        print('{}: {}'.format(metric, metrics[metric]))
        break

    if not args.no_save_np:
        save_to_np(args, metrics, avg_layer)

if __name__ == '__main__':
    main()
