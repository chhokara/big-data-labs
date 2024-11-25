from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+


def date2dayofyear_query(table_name='__THIS__'):
    query = """
        SELECT *, dayofyear(date) AS day_of_year
        FROM {table_name}
    """.format(table_name=table_name)
    return query


def yesterday_query(table_name='__THIS__'):
    query = """
        SELECT today.*, yesterday.tmax AS yesterday_tmax
        FROM {table_name} AS today
        INNER JOIN {table_name} AS yesterday
            ON date_sub(today.date, 1) = yesterday.date
            AND today.station = yesterday.station
    """.format(table_name=table_name)
    return query


def main(inputs, output):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])

    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    date_to_day_of_year_query = date2dayofyear_query()
    dayofyearSQLTrans = SQLTransformer(statement=date_to_day_of_year_query)

    y_query = yesterday_query()
    yesterdaySQLTrans = SQLTransformer(statement=y_query)

    weather_assembler = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation',
                   'day_of_year', 'yesterday_tmax'],
        outputCol="features_weather",
        handleInvalid="keep"
    )

    weather_regression = RandomForestRegressor(
        labelCol="tmax", featuresCol="features_weather", maxDepth=10, maxBins=40)

    weather_pipeline = Pipeline(
        stages=[dayofyearSQLTrans, yesterdaySQLTrans, weather_assembler, weather_regression])
    weather_model = weather_pipeline.fit(train)

    weather_results = weather_model.transform(validation)

    weather_results.show()

    evaluator_r2 = RegressionEvaluator(labelCol="tmax", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="tmax", metricName="rmse")

    score_r2 = evaluator_r2.evaluate(weather_results)
    print("r2: ", score_r2)

    score_rmse = evaluator_rmse.evaluate(weather_results)
    print("rmse: ", score_rmse)

    weather_model.write().overwrite().save(output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
