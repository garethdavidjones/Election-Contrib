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

def main(data):

    transactions = data.map(lambda x: (x[5], [x[0]])).sample(False, 0.2)
    print transactions.take(5)
    from_2012 = transactions.filter(lambda x: x[1] == "1984").reduceByKey(lambda x, y: "")#.cache()
    not_from_2012 = transactions.filter(lambda x: x[1] != "1984").reduceByKey(lambda x, y: x + y)#.cache()
    print not_from_2012.take(5)
    relv_indv = from_2012.join(not_from_2012)
    print relv_indv.take(5)
    

if __name__ == '__main__':
    # contribDB_198
    file_in = "gs://election-data/raw_data/contribDB_198*.csv"
    sc = pyspark.SparkContext() 

    data = data_cleaning(file_in)

    main(data)