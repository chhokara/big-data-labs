from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# add more functions as necessary


def filterFields(line):
    record = json.loads(line)
    new_record = {key: record[key] for key in ["subreddit", "score", "author"]}
    return new_record


def positiveFilter(sub):
    return int(sub["score"]) > 0


def negativeFilter(sub):
    return int(sub["score"]) <= 0


def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    subreddits = text.map(filterFields).filter(
        lambda sub: 'e' in sub["subreddit"])

    subreddits.cache()

    positiveSubreddits = subreddits.filter(positiveFilter)
    negativeSubreddits = subreddits.filter(negativeFilter)

    positiveSubreddits.map(json.dumps).saveAsTextFile(output + "/positive")
    negativeSubreddits.map(json.dumps).saveAsTextFile(output + "/negative")


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit ETL')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
