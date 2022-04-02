from pyspark import SparkContext
from itertools import combinations
sc = SparkContext("local", "bookPairs")
lines = sc.textFile("/home/cs143/data/goodreads.user.books")
lines = lines.map(lambda x: (x.split(":")[1]).split(","))
pairs = lines.flatMap(lambda data: combinations(data,2)).filter(lambda data: data[0] < data[1])
pair1s = pairs.map(lambda pair: (pair, 1))
pairCounts = pair1s.reduceByKey(lambda a, b: a+b)
result = pairCounts.filter(lambda x: x[1] > 20)
result.saveAsTextFile("output")



