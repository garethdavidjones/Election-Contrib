import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
ZIP_COL = 18
STATE_COL = 17
COL = 16
OUTPUT_PATH = 'gs://cs123data/Output/' + "TopDonors.txt"
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

def main(RDD):

    partyAmt = data.map(lambda x: (x[28], float(x[AMT_COL]) ) )
    partyKeyVal = partyAmt.reduceByKey(add)
    topParty = partyKeyVal.sortBy(lambda x: -x[1])
    
    for party in topParty.collect():
        print party[0] + " : $ " + str(party[1])

    print ""
    print ""
    recpTypeAmt = data.map(lambda x: (x[29], float(x[AMT_COL]) ) )
    recpKeyVal = recpTypeAmt.reduceByKey(add)
    topRecp = recpKeyVal.sortBy(lambda x: -x[1])

    for typ in topRecp.collect():
        print typ[0] + " : $ " + str(typ[1])

    print ""
    print ""

if __name__ == '__main__':

    sc = pyspark.SparkContext()
    data = primary_data_cleaning()
    main(data)