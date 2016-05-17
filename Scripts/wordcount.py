
# wordcount.py

from operator import add

f = sc.textFile("README.md")
wc = f.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(add)
wc.saveAsTextFile("wc_out.txt")