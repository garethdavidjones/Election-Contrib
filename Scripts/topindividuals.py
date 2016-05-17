import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
OUTPUT_PATH = 'gs://cs123data/Output/' + "TopDonors"
PRIMARY_FILE_PATH = 'gs://cs123data/Data/contribDB_2004.csv'
SECONDARY_FILE_PATH = 'gs://cs123data/Data/contributor_cfscores_st_fed_1979_2012.csv'
CID_COL = 5
TOP_K = 20
NAME = 6
LAST_NAME = 7 
FIRST_NAME = 8
FULL_NAME = 12

def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning():

    lines = sc.textFile(PRIMARY_FILE_PATH)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

def form_key_val(x):

    name = x[NAME].upper()
    names = name.split(" ")
    return (x[CID_COL], (float(x[AMT_COL]), set(names) ))

def my_reduce(a, b):

    return (a[0] + b[0], a[1].union(b[1]))

def main(RDD):

    contrAmt = data.map(form_key_val)
    contrKeyVal = contrAmt.reduceByKey(my_reduce)
    topContr = contrKeyVal.sortBy(lambda x: -x[1][0])
    topN = topContr.take(TOP_K)

    print topN

if __name__ == '__main__':

    sc = pyspark.SparkContext()

    data = primary_data_cleaning()

    topN = main(data)
    print topN

    with open(OUTPUT_PATH, 'wb') as f: 
        for person in topN:
            output = person[1][1] + ":  $ " + person[1][0]
            f.write(output + '\n')





    

