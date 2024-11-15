from pyspark.sql import SparkSession, functions as F, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(topic):
    # main logic starts here
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()

    values = messages.select(messages['value'].cast('string').alias("point"))
    values = values.withColumn("xi", F.split(
        values.point, ' ').getItem(0).cast('float'))
    values = values.withColumn("yi", F.split(
        values.point, ' ').getItem(1).cast('float'))
    values = values.withColumn("one", F.lit(1))
    values = values.withColumn("xi_yi", F.col("xi") * F.col("yi"))
    values = values.withColumn("xi_squared", F.col("xi") * F.col("xi"))

    values = values.groupBy().agg(F.sum(F.col("xi_yi")).alias("sum_x_y"),
                                  (F.lit(1) / F.sum(F.col("one"))).alias("1_over_n"),
                                  F.sum(F.col("xi")).alias("sum_x"),
                                  F.sum(F.col("yi")).alias("sum_y"),
                                  F.sum(F.col("xi_squared")).alias(
                                      "sum_x_squared"),
                                  F.sum(F.col("one")).alias("n"))

    values = values.withColumn("beta", (F.col("sum_x_y") - F.col("1_over_n") * F.col("sum_x") * F.col(
        "sum_y")) / (F.col("sum_x_squared") - F.col("1_over_n") * F.pow(F.col("sum_x"), F.lit(2))))
    values = values.withColumn("alpha", (F.col(
        "sum_y") / F.col("n")) - ((F.col("beta") * F.col("sum_x")) / F.col("n")))
    values = values.select("beta", "alpha")

    query = values.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination(600)


if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('reading streams').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)
