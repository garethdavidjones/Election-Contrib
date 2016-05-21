import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys
import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
OUTPUT_PATH = 'gs://cs123data/Output/'
INPUT_PATH = 'gs://cs123data/Data/contribDB_1984.csv'
CID_COL = 5
TOP_K = 20
CONTR_CF = 36

count = 0

def csv_parser(line):   

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def data_cleaning(file_in):

    lines = sc.textFile(file)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

'''
The objective of this function is to
    1. compare number of donations at particular ideology level
    2. The total amount of contrbutions at that ideology level

    Display:
        Histogram:
            Kernel Density Plot

'''
def float_filter(line):

    try:
        rv = round(float(line[CONTR_CF]),1)
        return True

    except:
        return False
        
 
def count_mapper(line):

    return (round(float(line[CONTR_CF]),1), 1)

def amount_mapper(line):

    return (round(float(line[CONTR_CF]),1), float(line[AMT_COL]))

def main(file):

    data = data_cleaning(file)

    CF_Float_Filter = data.filter(float_filter)

    CF_Amt = CF_Float_Filter.map(amount_mapper)
    CF_Count = CF_Float_Filter.map(count_mapper)

    Count_Reduce = CF_Count.reduceByKey(add).collect()
    Amt_Reduce = CF_Amt.reduceByKey(add).collect()

    return Count_Reduce, Amt_Reduce

if __name__ == '__main__':

    sc = pyspark.SparkContext()
    years = list(range(1980,2014, 2))
    partial_path = 'gs://cs123data/Data/contribDB_' 
    final_output = []
    for year in years:
        file = partial_path + str(year) + ".csv" 
        file_output = main(file)
        for bit in file_output:
            final_output.append(bit)
    
    for result in final_output:
        print(result)

        