import sys
import pyspark
import csv
from StringIO import StringIO
import numpy as np
from pyspark.sql import SQLContext

def get_sql(filename):
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', mode = 'DROPMALFORMED').load(filename)

    return df

def join_dfs(df1, df2):

    return(df1.join(df2, df1.ZCTA == df2.ZCTA, 'outer'))

def main(file1, file2):

    df1 = get_sql(file1)
    df2 = get_sql(file2)

    return(join_dfs(df1, df2))

if __name__ == '__main__':

    sqlContext = SQLContext(sc)
    
    df = main("gs://cs123data/Data/full_data.csv", "gs://cs123data/Data/updated_merger_3.csv")

    try:
        df.save('full_data_with_census.csv', 'com.databricks.spark.csv')
    except:
        df.write.format('com.databricks.spark.csv').save('full_data_with_census.csv')


#run gs://cs123data/Scripts/merger_3.py