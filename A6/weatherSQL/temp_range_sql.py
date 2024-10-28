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
    weather.createOrReplaceTempView("weather")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_filtered AS 
        SELECT * FROM weather 
        WHERE qflag IS NULL
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_tmin AS 
        SELECT date AS tmin_date, station AS tmin_station, value AS tmin
        FROM weather_filtered 
        WHERE observation = 'TMIN'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_tmax AS 
        SELECT date AS tmax_date, station AS tmax_station, value AS tmax
        FROM weather_filtered
        WHERE observation = 'TMAX'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_joined AS 
        SELECT tmax_date, tmax_station, tmax, tmin
        FROM weather_tmax w_tmax
        JOIN weather_tmin w_tmin
        ON w_tmax.tmax_date = w_tmin.tmin_date AND w_tmax.tmax_station = w_tmin.tmin_station
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_range AS 
        SELECT tmax_date AS date, tmax_station AS station, (tmax - tmin) / 10 AS range
        FROM weather_joined
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW weather_range_max AS
        SELECT date AS max_date, MAX(range) AS max_range
        FROM weather_range
        GROUP BY max_date
    """)

    res = spark.sql("""
        SELECT date, station, range
        FROM weather_range w_range
        JOIN weather_range_max w_range_max
        ON w_range.date = w_range_max.max_date AND w_range.range = w_range_max.max_range
        ORDER BY w_range.date, w_range.station
    """)

    res.write.csv(output, mode="overwrite")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range SQL').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
