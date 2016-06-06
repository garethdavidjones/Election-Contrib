import pyspark
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

sc = pyspark.SparkContext(appName="RandomForest")

# Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLabeledPoints(sc, 'gs://cs123data/Output/AmountVectors2/')
# Split the data into training and test sets
trainingData, testData = data.randomSplit([0.7, 0.3])
trainingData.cache()
testData.cache()

model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                    numTrees=1000, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=3, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /\
    float(testData.count())

print('Test Mean Squared Error = ' + str(testMSE))
print('Learned regression forest model:')
print(model.toDebugString())

# Save and load model
model.save(sc, "target/tmp/myRandomForestRegressionModel")
sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
