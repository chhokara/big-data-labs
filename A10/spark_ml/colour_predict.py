from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, functions as F, types as T
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(
        inputCols=["R", "G", "B"], outputCol="features", handleInvalid="keep")

    word_indexer = StringIndexer(inputCol="word", outputCol="number")

    classifier = MultilayerPerceptronClassifier(
        layers=[3, 30, 11],
        maxIter=100,
        blockSize=128,
        seed=1234,
        labelCol="number",
        featuresCol="features"
    )

    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # TODO: create an evaluator and score the validation data

    results = rgb_model.transform(validation)

    evaluator = MulticlassClassificationEvaluator(labelCol="number")
    score = evaluator.evaluate(results)

    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])

    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    labSQLTrans = SQLTransformer(statement=rgb_to_lab_query)

    lab_assembler = VectorAssembler(
        inputCols=['labL', 'labA', 'labB'], outputCol="features_lab", handleInvalid="keep")

    lab_word_indexer = StringIndexer(inputCol="word", outputCol="number")

    classifier_lab = MultilayerPerceptronClassifier(
        layers=[3, 30, 11],
        maxIter=100,
        blockSize=128,
        seed=1234,
        labelCol="number",
        featuresCol="features_lab"
    )

    lab_pipeline = Pipeline(
        stages=[labSQLTrans, lab_assembler, lab_word_indexer, classifier_lab])
    lab_model = lab_pipeline.fit(train)

    lab_results = lab_model.transform(validation)

    lab_evaluator = MulticlassClassificationEvaluator(labelCol="number")
    lab_score = lab_evaluator.evaluate(lab_results)

    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', lab_score)


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
