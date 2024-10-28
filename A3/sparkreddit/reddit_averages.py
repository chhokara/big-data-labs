from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# add more functions as necessary


def subreddit_countscore_pair(line):
    record = json.loads(line)
    subredditName = record['subreddit']
    score = record['score']
    return (subredditName, (1, score))


def add_pairs(pair_a, pair_b):
    count_sum = pair_a[0] + pair_b[0]
    score_sum = pair_a[1] + pair_b[1]

    return (count_sum, score_sum)


def compute_average(kv):
    subreddit, countscore = kv
    return (subreddit, countscore[1] / countscore[0])


def get_key(kv):
    return kv[0]


def output_format(jsonobj):
    return json.dumps(jsonobj)


def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    subreddits = text.map(subreddit_countscore_pair)
    subreddit_countscore = subreddits.reduceByKey(add_pairs)
    subreddit_avg = subreddit_countscore.map(compute_average)

    outdata = subreddit_avg.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
