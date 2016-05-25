
import pyspark
from pyspark.mllib.regression import LabeledPoint

def format_data(input_file):

    data = sc.textFile(input_file)
    print data.first()

def main(input_file):

    sc = pyspark.SparkContext(appName="DecisionTree")
    data = sc.textFile(input_file)
    labeledData = data.map(lambda x: LabeledPoint(x[0], x[1]))
    print "labeled_data", labeledData.first()
    # (trainingData, testData) = lableled_data.randomSplit([0.7, 0.3])

    # format_data(sc, input_file)

if __name__ == '__main__':
    
    input_file = "gs://cs123data/Output/checkpoint.text"
    main(input_file)