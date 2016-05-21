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
