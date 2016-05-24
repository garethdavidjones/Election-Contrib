#dataCleaner.py
import csv
from StringIO import StringIO
from operator import add


def csv_parser(line):   

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def data_cleaning(file_in):

    lines = sc.textFile(file_in)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove header lines

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 
    
    return data