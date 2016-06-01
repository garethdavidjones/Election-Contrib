import sys
import pyspark
import csv
from StringIO import StringIO
import numpy as np

def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning(file_path):

    lines = sc.textFile(file_path)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) != 1) 

    return data


def get_keyVals(x):

    zcode = x[1]

    cat_data = x[263:]

    rv = (zcode, cat_data)

    return rv

def main(data):

    return data.map(get_keyVals)

if __name__ == '__main__':

    sc = pyspark.SparkContext()
    main_file_path = "gs://cs123data/Data/updated_merger_3.csv"

    data = primary_data_cleaning(main_file_path)

    keyVals = main(data)

    keyVals.saveAsTextFile("gs://cs123data/Output/income_cat_rdd.txt")
