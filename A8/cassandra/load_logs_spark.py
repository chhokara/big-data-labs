from datetime import datetime
import uuid
from pyspark.sql import SparkSession, functions as F, types
import sys
import re
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def parse_log(line):
    line_re = re.compile(
        r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    parsed = line_re.match(line)
    if parsed:
        return {
            "id": str(uuid.uuid4()),
            "host": parsed.group(1),
            "datetime": datetime.strptime(parsed.group(2), "%d/%b/%Y:%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
            "path": parsed.group(3),
            "bytes": int(parsed.group(4))
        }
    return None


def main(input, keyspace, table):
    nasa_logs = sc.textFile(input)
    nasa_logs = nasa_logs.map(parse_log).filter(lambda log: log != None)

    log_schema = types.StructType([
        types.StructField('id', types.StringType()),
        types.StructField('host', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType())
    ])

    nasa_logs = spark.createDataFrame(nasa_logs, log_schema)
    nasa_logs = nasa_logs.repartition(500)
    nasa_logs.write.format("org.apache.spark.sql.cassandra").options(
        table=table, keyspace=keyspace).mode("append").save()


if __name__ == "__main__":
    input = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load logs into Cassandra').config(
        'spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, keyspace, table)
