from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(inputs, output):
    # main logic starts here
    comments_schema = types.StructType([
        types.StructField('archived', types.BooleanType()),
        types.StructField('author', types.StringType()),
        types.StructField('author_flair_css_class', types.StringType()),
        types.StructField('author_flair_text', types.StringType()),
        types.StructField('body', types.StringType()),
        types.StructField('controversiality', types.LongType()),
        types.StructField('created_utc', types.StringType()),
        types.StructField('distinguished', types.StringType()),
        types.StructField('downs', types.LongType()),
        types.StructField('edited', types.StringType()),
        types.StructField('gilded', types.LongType()),
        types.StructField('id', types.StringType()),
        types.StructField('link_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('parent_id', types.StringType()),
        types.StructField('retrieved_on', types.LongType()),
        types.StructField('score', types.LongType()),
        types.StructField('score_hidden', types.BooleanType()),
        types.StructField('subreddit', types.StringType()),
        types.StructField('subreddit_id', types.StringType()),
        types.StructField('ups', types.LongType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('month', types.IntegerType()),
    ])

    comments = spark.read.json(inputs, comments_schema)
    averages = comments.groupBy("subreddit").avg("score")
    averages.write.csv(output, mode="overwrite")
    averages.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Reddit DataFrames').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
