from math import sqrt
from pyspark.sql import SparkSession, functions as F, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(keyspace, table):
    nasa_logs = spark.read.format("org.apache.spark.sql.cassandra").options(
        table=table, keyspace=keyspace).load()
    nasa_logs = nasa_logs.groupBy(nasa_logs['host']).agg(F.count(
        'host').alias('count_requests'), F.sum('bytes').alias('sum_request_bytes'))
    nasa_logs = nasa_logs.withColumn("one", F.lit(1))
    nasa_logs = nasa_logs.withColumn("xi", F.col("count_requests"))
    nasa_logs = nasa_logs.withColumn(
        "xi_squared", F.pow(F.col('count_requests'), 2))
    nasa_logs = nasa_logs.withColumn("yi", F.col("sum_request_bytes"))
    nasa_logs = nasa_logs.withColumn(
        "yi_squared", F.pow(F.col('sum_request_bytes'), 2))
    nasa_logs = nasa_logs.withColumn("xi_yi", F.col(
        "count_requests") * F.col("sum_request_bytes"))

    n = nasa_logs.agg(F.sum('one')).collect()[0][0]
    sum_xi_yi = nasa_logs.agg(F.sum('xi_yi')).collect()[0][0]
    sum_xi = nasa_logs.agg(F.sum('xi')).collect()[0][0]
    sum_yi = nasa_logs.agg(F.sum('yi')).collect()[0][0]
    sum_xi_squared = nasa_logs.agg(F.sum('xi_squared')).collect()[0][0]
    sum_yi_squared = nasa_logs.agg(F.sum('yi_squared')).collect()[0][0]

    r = ((n * sum_xi_yi) - (sum_xi * sum_yi)) / (sqrt((n * sum_xi_squared) -
                                                      (sum_xi) ** 2) * sqrt((n * sum_yi_squared) - (sum_yi) ** 2))

    print("r: ", r)
    print("r squared: ", r ** 2)


if __name__ == "__main__":
    keyspace = sys.argv[1]
    table = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load logs into Cassandra').config(
        'spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table)
