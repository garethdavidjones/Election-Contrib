import pyspark
import csv
from StringIO import StringIO
from operator import add

def csv_parser(line):   

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        rv[36] = float(rv[36])
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

def ideo_bucketing(line):

    cf = line[36]

    key = round(cf,1)
    return (key, 1)


def main(data):

    sampleData = data.sample(False, 0.1, 66)
    transactions = sampleData.map(ideo_bucketing)
    transCount = transactions.reduceByKey(add)
    output = transCount.collect()
    print(output)
    
if __name__ == '__main__':

    file_in = "gs://cs123data/Data/full_data.csv"
    sc = pyspark.SparkContext() 

    data = data_cleaning(file_in)

    main(data)