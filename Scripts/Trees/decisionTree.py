
import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel

def main(input_file):

    sc = pyspark.SparkContext(appName="DecisionTree")

    data = MLUtils.loadLabeledPoints(sc, input_file)

    trainingData, testData = data.randomSplit([0.70, 0.3])
    # Cache in memory for faster training
    trainingData.cache()

    model = DecisionTree.trainClassifier(trainingData, numClasses=4, impurity='gini',
                 categoricalFeaturesInfo={}, maxDepth=16, maxBins=10)

    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    # print tree_model.toDebugString()
    print ""
    print ""
    print "Test Erros: {}".format(round(testErr,4))


if __name__ == '__main__':
   

    input_file = "gs://cs123data/Output/PartyVectors/"
    main(input_file)
