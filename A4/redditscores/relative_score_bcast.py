from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# add more functions as necessary


def compute_relative_score(comment, broadcast_avg):
    avg = broadcast_avg.value.get(comment[0], None)

    if avg == None:
        return None
    else:
        commentdata = comment[1]

        relative_score = float(commentdata["score"]) / float(avg)
        author = commentdata["author"]

        return (relative_score, author)


def filterFields(line):
    record = json.loads(line)
    new_record = {key: record[key] for key in ["subreddit", "score", "author"]}
    return new_record


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


def output_format(kv):
    k, v = kv
    return '%f %s' % (k, v)


def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)

    subreddits = text.map(subreddit_countscore_pair)
    subreddit_countscore = subreddits.reduceByKey(add_pairs)
    subreddit_avg = subreddit_countscore.map(compute_average)
    positive_avg = subreddit_avg.filter(lambda kv: kv[1] > 0)
    positive_avg_broadcast = sc.broadcast(dict(positive_avg.collect()))

    comments = text.map(filterFields)
    comments.cache()
    commentsbysub = comments.map(lambda c: (c["subreddit"], c))

    result = commentsbysub.map(lambda comment: compute_relative_score(
        comment, positive_avg_broadcast)).filter(lambda x: x != None)

    outdata = result.sortByKey(ascending=False).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
