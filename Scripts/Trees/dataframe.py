import sys
import pyspark
import csv
from StringIO import StringIO
from operator import add
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import com.databricks.spark.csv




def main(main_file):


    sc = pyspark.SparkContext(appName="ContributionPrediction")
    sqlContext = SQLContext(sc)
    data = sqlContext.read.load(main_file, 
                              format='com.databricks.spark.csv', 
                              header='true', 
                              inferSchema='true')

    print data.types
    print data.describe()

    # full_data = data_cleaning(sc, main_file)
    # # test_data = data_cleaning(sc, test_file)

    # data_2012 = full_data.filter(lambda x : x[0] == 1980 ) #Should probably filter out other transaction types
    # print data_2012.first()
    # evaluated_data = data_2012.map(evaluate_contribution)
    # print evaluated_data.first()
    # combined_evaluations = evaluated_data.reduceByKey(lambda x, y: x) # An RDD of Unique Keys 
    # print combined_evaluations.first()

    # all_individuals = full_data.map(build_features)
    # print all_individuals.first()
    # non_contributors = all_individuals.subtractByKey(combined_evaluations)
    # print non_contributors.first() 

    # train_indv = contributions.reduceByKey(reduce_individuals)
    
if __name__ == '__main__':
    

    main_file = "gs://cs123data/Data/contribDB_1996.csv"
    main(main_file)