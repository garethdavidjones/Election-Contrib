import pyspark
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import csv
from StringIO import StringIO

sc = pyspark.SparkContext()


def parser(x):

    rv = list(csv.reader(x, delimiter=","))

    i = 0

    for data in rv:
        try:
            data = float(data)
        except:
            data = 0
    
        rv[i] = data
        i = i + 1

    return rv


#from : http://spark.apache.org/docs/latest/mllib-clustering.html
# Load and parse the data
data = sc.textFile("gs://cs123data/Data/updated_merger_3.csv")
parsedData = data.map(parser)

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 10, maxIterations=10,
        runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
clusters.save(sc, "gs://cs123data/Data/updated_merger_3_clusters.csv")

#gs://cs123data/Scripts/clusterer.py