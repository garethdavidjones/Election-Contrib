
import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel

def main(input_file):

    sc = pyspark.SparkContext(appName="DecisionTree")

    data = MLUtils.loadLabeledPoints(sc, input_file)

    trainingData, testData, CVdata = data.randomSplit([0.70, 0.15, 0.15])
    # Cache in memory for faster training
    trainingData.cache()
    testData.cache()
    CVdata.cache()
    bar = trainingData.first()
    num_features = len(bar.features) - 4
    base_feature_count = 9
    features = {3:3, 4:7, 5:4, 6:2, 7:3, 8:3}
    
    # for i in range(base_feature_count + 1, num_features + base_feature_count + 1):
    #     features[i] = 10

    tree_model = DecisionTree.trainClassifier(trainingData, numClasses=3, impurity='gini',
                 categoricalFeaturesInfo=features, maxDepth=4, maxBins=10)

    predictions = tree_model.predict(testData.map(lambda x: x.features))
    labels_and_preds = testData.map(lambda x: x.label).zip(predictions)    

    test_accuracy = labels_and_preds.filter(lambda (y, x): y == x).count() / float(testData.count())

    print tree_model.toDebugString()
    print "Test accuracy: {}".format(round(test_accuracy,4))


if __name__ == '__main__':
   

    input_file = "gs://cs123data/Output/LabeledVectors3/"
    main(input_file)
