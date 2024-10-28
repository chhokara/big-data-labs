from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def wikis_attrs(line):
    items = line.split()

    dt_string = items[0]
    locale = items[1]
    wiki_name = items[2]
    visit_count = int(items[3])

    return (dt_string, locale, wiki_name, visit_count)


def get_maximum(value_pair_a, value_pair_b):
    if value_pair_a[0] >= value_pair_b[0]:
        return value_pair_a
    else:
        return value_pair_b


def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


text = sc.textFile(inputs)
wikis = text.map(wikis_attrs)
filteredwikis = wikis.filter(
    lambda attrs: attrs[1] == "en" and attrs[2] != "Main_Page" and not attrs[2].startswith("Special:"))
wiki_kv_pairs = filteredwikis.map(
    lambda attrs: (attrs[0], (attrs[3], attrs[2])))
max_count = wiki_kv_pairs.reduceByKey(get_maximum)

outdata = max_count.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)
