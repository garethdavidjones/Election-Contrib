import pyspark
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import csv
from StringIO import StringIO

sc = pyspark.SparkContext()


def to_float(x):

    i = 0

    for col in x:
        try:
            col = float(col)
        except:
            col = 0

        x[i] = col
        i = i + 1

def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        rv = rv[6:175] + rv[179:]
        if((rv == None) or (rv[2] == "NA")):
            rv = [1]
        try:
            rv = [int(i) for i in rv]
        except:
            rv = [1]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning(file_path):

    lines = sc.textFile(file_path)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) != 1)
    data.saveAsTextFile("gs://cs123data/Output/merge.txt")

    data = data.map(to_float)

    return data


#from : http://spark.apache.org/docs/latest/mllib-clustering.html
# Load and parse the data
data = primary_data_cleaning("gs://cs123data/Data/updated_merger_3.csv")
# Build the model (cluster the data)
clusters = KMeans.train(data, 10, maxIterations=10,
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