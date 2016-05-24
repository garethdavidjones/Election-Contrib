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

def amount_buketing(line):

    amt = float(line[3])

    if amt < 200:
        key = "Tiny"
    elif (amt > 200) and (amt < 1000):
        key = "Small"   
    elif (amt >= 1000) and (amt <10000):
        key = "Medium"
    elif (amt >= 1000) and (amt <10000):
        key = "Medium-Large"    
    elif (amt >= 10000) and (amt <50000):
        key = "Large"
    elif (amt >= 50000) and (amt < 100000):
        key = "Very Large"
    elif (amt >= 100000) and (amt < 500000):
        key = "Massive"
    else:
        key = "Collosal"

    return (key, 1)

def main(data):

    sampleData = data.sample(False, 0.1, 66)
    transactions = sampleData.map(amount_buketing)
    transCount = transactions.reduceByKey(add)
    output = transCount.collect()
    print(output)
    
if __name__ == '__main__':

    file_in = "gs://cs123data/Data/full_data.csv"
    sc = pyspark.SparkContext() 

    data = data_cleaning(file_in)

    main(data)