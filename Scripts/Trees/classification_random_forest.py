import pyspark
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

sc = pyspark.SparkContext(appName="RandomForest")

# Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLabeledPoints(sc, 'gs://cs123data/Output/PartyVectors/')
# Split the data into training and test sets
trainingData, testData = data.randomSplit([0.7, 0.3])
trainingData.cache()

# The depth of the tree proved to be a significant bottle neck
model = RandomForest.trainClassifier(trainingData, numClasses=4, categoricalFeaturesInfo={},
                                     numTrees=700, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=8, maxBins=12)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print("")
print("")
print('Test Error: ' + str(testErr))