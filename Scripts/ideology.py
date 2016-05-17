
import seaborn as sns
import pandas as pd

import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
OUTPUT_PATH = 'gs://cs123data/Output/'
INPUT_PATH = 'gs://cs123data/Data/contribDB_2004.csv'
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

def data_cleaning():

    lines = sc.textFile(INPUT_PATH)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

'''
The objective of this function is to
    1. compare number of donations at particular ideology level
    2. The total amount of contrbutions at that ideology level

    Display:
        Histogram:
            Kernel Density Plot

'''

def main(data):

    CF_Count = data.map(lambda x: (round(float(x[CONTR_CF]), 1), 1) )
    CF_Amt = data,data.map(lambda x: (round(float(x[CONTR_CF]), 1), float(x[AMT_COL])) )

    Count_Reduce = CF_Count.reduceByKey(add).collect()
    Amt_Reduce = CF_Amt.reduceByKey(add).collect()

    print Count_Reduce[:10]

    Count_df = pd.DataFrame(Count_Reduce, ["CF", "Count"])
    Amt_df = pd.DataFrame(Amt_Reduce, ["CF", "Amount"])

    print Count_df.head()
    print Amt_df.head()

    count_fig = sns.barplot("CF", "Count", data=Count_df)
    amt_fig = sns.barplot("CF", "Amount", data=Amt_df)

    count_fig.savefig(OUTPUT_PATH)
    amt_fig.savefig(OUTPUT_PATH)


if __name__ == '__main__':

    sc = pyspark.SparkContext()
    data = data_cleaning()
    main(data)