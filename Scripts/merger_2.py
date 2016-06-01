import sys
import pyspark
import csv
from StringIO import StringIO
import numpy as np

#normal parsing and cleaning

def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning(file_path_1):

    lines = sc.textFile(file_path_1)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

def sec_data_cleaning(file_path_2):

    lines = sc.textFile(file_path_2)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) != 1) 

    return data

def main_join(file_1, file_2):

	RDD_1 = primary_data_cleaning(file_1)
	RDD_2 = sec_data_cleaning(file_2)

	out = RDD_1