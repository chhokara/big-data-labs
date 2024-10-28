import re
from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    file_name_components = path.split('/')
    pattern = r"(\d{8}-\d{2})"
    date_hour = re.search(pattern, file_name_components[-1])
    return date_hour.group(1)


def main(inputs, output):
    # main logic starts here
    wiki_schema = types.StructType([
        types.StructField('locale', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.IntegerType()),
        types.StructField('bytes', types.LongType())
    ])
    data = spark.read.csv(inputs, wiki_schema, ' ').withColumn(
        'filename', functions.input_file_name())
    wikis = data.withColumn('hour', path_to_hour(data.filename))
    all_wikis = wikis.filter((wikis['locale'] == 'en') & (
        wikis['title'] != 'Main_Page') & (~wikis['title'].startswith('Special:')))
    all_wikis.cache()
    max_wikis = all_wikis.groupby('hour').agg(functions.max('views'))
    max_wikis.cache()
    max_wikis = max_wikis.withColumnRenamed('hour', 'max_wikis_hour')
    res = all_wikis.join(max_wikis.hint("broadcast"), (max_wikis['max(views)'] == all_wikis['views']) & (
        max_wikis['max_wikis_hour'] == all_wikis['hour']))
    res.explain()
    res.select('hour', 'title', 'views').orderBy(
        'hour', 'views').write.json(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wiki dataframes').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
