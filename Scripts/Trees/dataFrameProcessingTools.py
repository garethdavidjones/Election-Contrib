# The dataframe implementation
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

fileName = ""

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(fileName)
df.select('year', 'model').write.format('com.databricks.spark.csv').save('newcars.csv')