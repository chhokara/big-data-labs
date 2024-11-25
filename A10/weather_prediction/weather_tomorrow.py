import datetime
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+


def main(model):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])

    weather_model = PipelineModel.load(model)

    station = 'sfu-station'
    day_of_date = datetime.date(2024, 11, 22)
    day_after_date = datetime.date(2024, 11, 23)
    latitude = 49.2771
    longitude = -122.9146
    elevation = 330.0
    day_of_tmax = 12.0
    day_after_tmax = 8.61

    input_data = [
        (station, day_after_date, latitude, longitude, elevation, day_after_tmax),
        (station, day_of_date, latitude, longitude, elevation, day_of_tmax)
    ]

    input = spark.createDataFrame(input_data, schema=tmax_schema)
    input.show()

    results = weather_model.transform(input)
    results.show()
    prediction = results.select("prediction").collect()[0][0]
    print('Predicted tmax tomorrow:', prediction)


if __name__ == '__main__':
    model = sys.argv[1]
    main(model)
