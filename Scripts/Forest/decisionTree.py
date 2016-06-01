
import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util.MLUtils import loadLabeledPoints 


def main(input_file):

    sc = pyspark.SparkContext(appName="DecisionTree")

    data = loadLabeledPoints(sc, input_file)

    trainingData, testData, CVdata = formated.randomSplit([0.70, 0.15, 0.15])
    # Cache in memory for faster training
    traniningData.cache()
    testData.cache()
    CVdata.cache()

    tree_model = DecisionTree.trainClassifier(training_data, numClasses=3, impurity='gini', maxDepth=16, maxBins=10)


    predictions = tree_model.predict(test_data.map(lambda x: x.features))
    labels_and_preds = test_data.map(lambda x: x.label).zip(predictions)    

    test_accuracy = labels_and_preds.filter(lambda (y, x): y == x).count() / float(test_data.count())

    print tree_model.toDebugString()
    print "Test accuracy: {}".format(round(test_accuracy,4))


if __name__ == '__main__':
   

    input_file = "gs://cs123data/"
    main(input_file)