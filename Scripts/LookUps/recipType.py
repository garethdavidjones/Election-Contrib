import pyspark
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

def main(stuff):

    sampleData = stuff.sample(False, 0.05, 66)
    transactions = sampleData.map(lambda x: (x[29], 1))
    transCount = transactions.reduceByKey(add)
    output = transCount.collect()
    print(output)
    
if __name__ == '__main__':

    file_in = "gs://cs123data/Data/full_data.csv"
    sc = pyspark.SparkContext() 

    data = data_cleaning(file_in)

    main(data)