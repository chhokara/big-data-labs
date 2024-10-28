from pyspark import SparkConf, SparkContext
import sys
import re
import string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    tokens = wordsep.split(line.lower())
    for t in tokens:
        yield (t, 1)


def add(x, y):
    return x + y


def get_key(kv):
    return kv[0]


def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    text = text.repartition(80)
    words = text.flatMap(words_once)
    filteredwords = words.filter(lambda kv: kv[0] != "")
    wordcount = filteredwords.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('spark wordcount improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
