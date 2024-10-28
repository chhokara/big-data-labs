from math import sqrt
from pyspark.sql import SparkSession, functions as F, types
import sys
import re
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary
def parse_log(line):
    line_re = re.compile(
        r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    parsed = line_re.match(line)
    if parsed:
        return {
            "host_name": parsed.group(1),
            "datetime": parsed.group(2),
            "path": parsed.group(3),
            "bytes": int(parsed.group(4))
        }
    return None


def main(inputs):
    # main logic starts here
    nasa_logs = sc.textFile(inputs)
    nasa_logs = nasa_logs.map(parse_log).filter(lambda log: log != None)

    log_schema = types.StructType([
        types.StructField('host_name', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType())
    ])

    nasa_logs = spark.createDataFrame(nasa_logs, log_schema)
    nasa_logs = nasa_logs.groupBy(nasa_logs['host_name']).agg(F.count(
        'host_name').alias('count_requests'), F.sum('bytes').alias('sum_request_bytes'))
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


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('nasa logs').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)
