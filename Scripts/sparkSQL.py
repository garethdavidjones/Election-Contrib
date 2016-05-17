
from spark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

FILE_PATH = 'gs://cs123data/Data/contribDB_1980.csv'

# df1980 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('gs://cs123data/Data/contribDB_1980.csv')

df2004 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('gs://cs123data/Data/contribDB_2004.csv')
df2006 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('gs://cs123data/Data/contribDB_2006.csv')
df2008 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('gs://cs123data/Data/contribDB_2008.csv')
df2010 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('gs://cs123data/Data/contribDB_2010.csv')

df2004.describe().show()
df2006.describe().show()
df2008.describe().show()
df2010.describe().show()


Output:

    2006:
        