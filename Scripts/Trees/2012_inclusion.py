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

    # print data.first()
    transactions = data.map(lambda x: (x[5], x[0])).sample(False, 0.2)
    # print transactions.take(5)
    from_2012 = transactions.filter(lambda x: x[1] == "2012").reduceByKey(lambda x, y: "")#.cache()
    not_from_2012 = transactions.filter(lambda x: x[1] != "2012").reduceByKey(lambda x, y: "")#.cache()
    print "from_2012", from_2012.take(5)
    print "not_from_2012", not_from_2012.take(5)

    from_2012_count = from_2012.count()
    not_from_2012_count = not_from_2012.count()
    print "2012 count:", from_2012_count
    print "not from 2012 count:", not_from_2012_count
    # Number of people not in 2012
    difference_count = not_from_2012.subtractByKey(from_2012).count()
    print "dif count", difference_count
    ratio = float(difference_count) / not_from_2012_count

    print "Percent Not in 2012"
    print ratio
    print 1 - ratio

    # Percent Not in 2012
    # 0.853187665049
    # 0.146812334951


if __name__ == '__main__':

    file_in = "gs://election-data/raw_data/*.csv"
    sc = pyspark.SparkContext() 

    data = data_cleaning(file_in)

    main(data)