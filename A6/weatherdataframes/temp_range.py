from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather = weather.filter(weather['qflag'].isNull())
    weather.cache()

    weather_tmin = weather.filter(weather['observation'] == 'TMIN')
    weather_tmin = weather_tmin.withColumnRenamed('value', 'tmin')

    weather_tmax = weather.filter(weather['observation'] == 'TMAX')
    weather_tmax = weather_tmax.withColumnRenamed('value', 'tmax')

    weather_joined = weather_tmax.join(weather_tmin, ['date', 'station'])

    weather_range = weather_joined.withColumn(
        'range', (weather_joined['tmax'] - weather_joined['tmin']) / 10)
    weather_range.cache()

    weather_range_max = weather_range.groupBy(
        'date').agg(functions.max('range'))
    weather_range_max = weather_range_max.withColumnRenamed('date', 'max_date')

    result = weather_range_max.join(weather_range, (weather_range['date'] == weather_range_max['max_date']) & (
        weather_range['range'] == weather_range_max['max(range)']))
    result = result.select(
        'date', 'station', 'range').orderBy('date', 'station')
    result.write.csv(output, mode="overwrite")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
