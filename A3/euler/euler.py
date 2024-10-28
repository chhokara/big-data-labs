from pyspark import SparkConf, SparkContext
import random
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# add more functions as necessary


def calculate_V(iterator):
    seed_value = random.randint(0, 1000)
    random.seed(seed_value)

    for _ in iterator:
        iterations = 0
        sum = 0.0
        while sum < 1:
            iterations += 1
            sum += random.random()
        yield iterations


def main(inputs):
    # main logic starts here
    n_samples = int(inputs)

    samples = sc.parallelize(range(n_samples), numSlices=32)
    V_values = samples.mapPartitions(calculate_V)
    total_iterations = V_values.reduce(lambda a, b: a + b)

    print(total_iterations / n_samples)


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler spark')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
