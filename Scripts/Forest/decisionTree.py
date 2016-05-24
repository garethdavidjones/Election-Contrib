
import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SQLContex



labeled_non_contributors = non_contributors.map(lambda x: LabeledPoint(0.0, x[1]))
labeled_contributors = contributors.map()
labeled_data = labeled_non_contributors.union(labeled_contributors)
(trainingData, testData) = lableled_data.randomSplit([0.7, 0.3])

def format_data(input_file):

    data = sc.textFile(input_file)


def main(input_file):

    sc = pyspark.SparkContext(appName="DecisionTree")


if __name__ == '__main__':
    
    input_file = "gs://cs123data/Output/checkpoint.text"
    main(input_file)