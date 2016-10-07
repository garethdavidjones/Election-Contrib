
import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
ZIP_COL = 18
STATE_COL = 17
CITY_COL = 16
FILE_PATH = 'gs://cs123data/Data/contribDB_2004.csv'

def csv_parser(line):
    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def data_cleaning():

    lines = sc.textFile(FILE_PATH)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

def main(RDD):

    cityAmt = data.map(lambda x: (x[CITY_COL], float(x[AMT_COL])))
    cityKeyVal = cityAmt.reduceByKey(add)
    topcities = cityKeyVal.sortBy(lambda x: -x[1])


if __name__ == '__main__':

    sc = pyspark.SparkContext()

    data = data_cleaning()

    main(data)