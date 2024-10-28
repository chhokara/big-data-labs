from pyspark.sql import SparkSession, functions as F, types, Row, Window
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary
def process_line(line):
    components = line.split(':')
    node = int(components[0])
    adjacent_nodes = components[1].strip().split()
    if len(adjacent_nodes) > 0:
        return [(node, int(adj)) for adj in adjacent_nodes]
    else:
        return [(node, None)]


def main(inputs, output, source, dest):
    # main logic starts here

    graph = sc.textFile(inputs + '/links-simple-sorted.txt')
    graph = graph.flatMap(process_line)

    edges_schema = types.StructType([
        types.StructField('src', types.IntegerType()),
        types.StructField('dest', types.IntegerType())
    ])
    edges = spark.createDataFrame(graph, edges_schema)
    edges = edges.filter(edges['dest'].isNotNull())
    edges.cache()

    paths_schema = types.StructType([
        types.StructField('node', types.IntegerType()),
        types.StructField('source', types.IntegerType()),
        types.StructField('distance', types.IntegerType()),
    ])
    paths = spark.createDataFrame(
        [(int(source), int(source), 0)], paths_schema)
    paths.cache()

    converged = False
    for i in range(6):
        new_paths = paths.alias('p').join(edges.alias('e'), F.col('p.node') == F.col('e.src')).select(F.col(
            "e.dest").alias('node'), F.col("e.src").alias('source'), (F.col("p.distance") + 1).alias('distance'))

        paths = paths.union(new_paths)
        window = Window.partitionBy(paths.node).orderBy(paths.distance)
        paths = paths.withColumn('order', F.row_number().over(
            window)).filter(F.col('order') == 1).drop('order')
        paths.cache()

        print(f"iteration {i+1}:")
        paths.show()
        paths.write.csv(output + '/iter-' + str(i), mode="overwrite")

        if paths.filter(F.col('node') == int(dest)).count() > 0:
            converged = True
            break

    if not converged:
        print("The path was not found")
        return

    source = int(source)
    dest = int(dest)

    reconstructed_path = [dest]

    cur = dest
    while cur != source:
        cur_source = paths.filter(F.col("node") == cur).select(
            F.col("source")).collect()[0][0]

        reconstructed_path.append(cur_source)
        cur = cur_source

    reconstructed_path.reverse()
    print("The reconstructed path:")
    for i in range(len(reconstructed_path)):
        print(reconstructed_path[i])

    final_path = sc.parallelize(reconstructed_path)
    final_path.saveAsTextFile(output + '/path')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    dest = sys.argv[4]
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, dest)
